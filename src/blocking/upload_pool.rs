use std::default::Default;
use std::fmt::Debug;
use std::sync::{mpsc, Arc, Mutex, MutexGuard};
use std::{thread, time};

use crate::blocking::aws::{AWS2Client, AWS4Client};
use crate::blocking::{AuthType, S3Client};
use crate::error::Error;
use log::{debug, info};

#[derive(Default)]
pub struct MultiUploadParameters {
    pub part_number: usize,
    pub payload: Vec<u8>,
}

pub struct UploadRequestPool {
    ch_data: Option<mpsc::Sender<Box<MultiUploadParameters>>>,
    ch_result: mpsc::Receiver<Result<(usize, reqwest::header::HeaderMap), Error>>,
    total_worker: usize,
    total_jobs: usize,
}

fn acquire<'a, T>(s: &'a Arc<Mutex<T>>) -> MutexGuard<'a, T>
where
    T: Debug,
{
    let mut l = s.lock();
    while l.is_err() {
        thread::sleep(time::Duration::from_millis(1000));
        info!("sleep and wait for lock... error: {:?}", l);
        l = s.lock();
    }
    l.expect("lock acuired")
}

impl UploadRequestPool {
    pub fn new(
        auth_type: AuthType,
        secure: bool,
        access_key: String,
        secret_key: String,
        host: String,
        uri: String,
        region: String,
        upload_id: String,
        total_worker: usize,
    ) -> Self {
        let (ch_s, ch_r) = mpsc::channel();
        let a_ch_r = Arc::new(Mutex::new(ch_r));
        let (ch_result_s, ch_result_r) = mpsc::channel();
        let a_ch_result_s = Arc::new(Mutex::new(ch_result_s));

        for _ in 0..total_worker {
            let a_ch_r2 = a_ch_r.clone();
            let a_ch_result_s2 = a_ch_result_s.clone();
            let upload = upload_id.clone();
            let akey = access_key.clone();
            let skey = secret_key.clone();
            let h = host.clone();
            let u = uri.clone();
            let r = region.clone();

            std::thread::spawn(move || loop {
                let s3_client: Box<dyn S3Client> = match auth_type {
                    AuthType::AWS2 => Box::new(AWS2Client {
                        tls: secure,
                        access_key: &akey,
                        secret_key: &skey,
                    }),
                    AuthType::AWS4 => Box::new(AWS4Client {
                        tls: secure,
                        access_key: &akey,
                        secret_key: &skey,
                        host: &h,
                        region: r.to_string(),
                    }),
                };
                let recv_end = a_ch_r2.lock().expect("worker recv end is expected");
                let result_send_back_ch = acquire(&a_ch_result_s2);
                loop {
                    let p: Box<MultiUploadParameters> = match recv_end.recv() {
                        Ok(p) => p,
                        Err(e) => {
                            let r = acquire(&a_ch_result_s2);
                            r.send(Err(Error::RequestPoolError(format!("{:?}", e))))
                                .ok();
                            drop(r);
                            return;
                        }
                    };
                    if p.part_number == 0 {
                        // Because part number is count from 1,
                        // 0 is used to close the channel
                        drop(recv_end);
                        drop(result_send_back_ch);
                        return;
                    }

                    match s3_client.request(
                        "PUT",
                        &h,
                        &u,
                        &mut vec![
                            ("uploadId", upload.as_str()),
                            ("partNumber", p.part_number.to_string().as_str()),
                        ],
                        &mut Vec::new(),
                        &p.payload,
                    ) {
                        Ok(r) => {
                            info!("Part {} uploading ...", p.part_number);
                            let mut send_result =
                                result_send_back_ch.send(Ok((p.part_number.clone(), r.2.clone())));
                            while send_result.is_err() {
                                info!("send back result error: {:?}", send_result);
                                thread::sleep(time::Duration::from_millis(1000));
                                send_result = result_send_back_ch
                                    .send(Ok((p.part_number.clone(), r.2.clone())));
                            }
                            info!("Part {} uploaded", p.part_number);
                        }
                        Err(e) => {
                            info!("Error on uploading Part {}: {}", p.part_number, e);
                            let rs = acquire(&a_ch_result_s2);
                            rs.send(Err(e)).expect("channel is full to handle messages");
                            drop(rs);
                        }
                    };
                }
            });
        }
        UploadRequestPool {
            ch_data: Some(ch_s),
            total_worker,
            ch_result: ch_result_r,
            total_jobs: 0,
        }
    }
    pub fn run(&mut self, p: MultiUploadParameters) {
        if let Some(ref ch_s) = self.ch_data {
            info!("sending part {} to worker", p.part_number);
            ch_s.send(Box::new(p))
                .expect("channel is full to handle messages");
            self.total_jobs += 1;
        }
    }
    pub fn close(&self) {
        let mut close_sent = 0;
        while let Some(ref ch_s) = self.ch_data {
            ch_s.send(Box::new(MultiUploadParameters {
                part_number: 0,
                ..Default::default()
            }))
            .expect("channel is full to handle messages");
            close_sent += 1;
            if close_sent == self.total_worker {
                info!("request pool closed");
                return;
            }
        }
    }
    pub fn wait(mut self) -> Result<String, Error> {
        let mut results = Vec::new();
        self.ch_data.take();
        loop {
            thread::sleep(time::Duration::from_millis(1000));
            let result = self
                .ch_result
                .recv()
                .expect("channel is full to handle messages");

            results.push(result);
            info!("{} parts uploaded", results.len());
            if results.len() == self.total_jobs {
                self.close();
                let mut content = format!("<CompleteMultipartUpload>");
                for res in results {
                    debug!("{:?}", res);
                    let r = res?;
                    let part = r.0;
                    let etag = r.1[reqwest::header::ETAG]
                        .to_str()
                        .expect("unexpected etag from server");

                    info!("part: {}, etag: {}", part, etag);
                    content.push_str(&format!(
                        "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                        part, etag
                    ));
                }
                content.push_str(&format!("</CompleteMultipartUpload>"));
                return Ok(content);
            }
        }
    }
}
