use std::default::Default;
use std::fmt::Debug;
use std::sync::{mpsc, Arc, Mutex, MutexGuard};
use std::{thread, time};

use crate::blocking::aws::{AWS2Client, AWS4Client};
use crate::blocking::{AuthType, S3Client};
use crate::error::Error;
use log::{debug, error, info};

#[derive(Default, Debug, Clone)]
pub struct MultiDownloadParameters(pub usize, pub usize);

pub struct DownloadRequestPool {
    ch_data: Option<mpsc::Sender<Box<MultiDownloadParameters>>>,
    ch_result: mpsc::Receiver<Result<MultiDownloadParameters, Error>>,
    total_worker: usize,
    total_jobs: usize,
    data: Arc<Mutex<Vec<u8>>>,
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

impl DownloadRequestPool {
    pub fn new(
        auth_type: AuthType,
        secure: bool,
        access_key: String,
        secret_key: String,
        host: String,
        uri: String,
        region: String,
        totoal_size: usize,
        total_worker: usize,
    ) -> Self {
        let (ch_s, ch_r) = mpsc::channel();
        let a_ch_r = Arc::new(Mutex::new(ch_r));
        let (ch_result_s, ch_result_r) = mpsc::channel();
        let a_ch_result_s = Arc::new(Mutex::new(ch_result_s));
        let data = Arc::new(Mutex::new(vec![0; totoal_size]));

        for _ in 0..total_worker {
            let a_ch_r2 = a_ch_r.clone();
            let a_ch_result_s2 = a_ch_result_s.clone();
            let akey = access_key.clone();
            let skey = secret_key.clone();
            let h = host.clone();
            let u = uri.clone();
            let r = region.clone();
            let d = data.clone();

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
                    let p: Box<MultiDownloadParameters> = match recv_end.recv() {
                        Ok(p) => p,
                        Err(e) => {
                            let r = acquire(&a_ch_result_s2);
                            r.send(Err(Error::RequestPoolError(format!("{:?}", e))))
                                .ok();
                            drop(r);
                            return;
                        }
                    };
                    if p.0 == 0 && p.1 == 0 {
                        // range(0, 0) is the stop signal
                        drop(recv_end);
                        drop(result_send_back_ch);
                        return;
                    }

                    info!("Range ({}, {}) downloading...", p.0, p.1);
                    match s3_client.request(
                        "GET",
                        &h,
                        &u,
                        &mut Vec::new(),
                        &mut vec![("range", &format!("bytes={}-{}", p.0, p.1 - 1))],
                        &Vec::new(),
                    ) {
                        Ok(r) => {
                            if r.1.len() == p.1 - p.0 {
                                let mut inner = acquire(&d);
                                inner[p.0..p.1].copy_from_slice(&r.1);
                                let mut send_result = result_send_back_ch.send(Ok((*p).clone()));
                                while send_result.is_err() {
                                    info!("send back result error: {:?}", send_result);
                                    thread::sleep(time::Duration::from_millis(1000));
                                    send_result = result_send_back_ch.send(Ok((*p).clone()));
                                }
                            } else {
                                error!(
                                    "Range ({}, {}) download size not correct {}",
                                    p.0,
                                    p.1,
                                    r.1.len()
                                );
                            }
                            info!("Range ({}, {}) download executed", p.0, p.1);
                        }
                        Err(e) => {
                            info!("Error on downloading Range ({}, {}): {}", p.0, p.1, e);
                            let rs = acquire(&a_ch_result_s2);
                            rs.send(Err(e)).expect("channel is full to handle messages");
                            drop(rs);
                        }
                    };
                }
            });
        }
        DownloadRequestPool {
            ch_data: Some(ch_s),
            total_worker,
            ch_result: ch_result_r,
            total_jobs: 0,
            data,
        }
    }
    pub fn run(&mut self, p: MultiDownloadParameters) {
        if let Some(ref ch_s) = self.ch_data {
            info!("sending range ({}, {}) request to worker", p.0, p.1);
            ch_s.send(Box::new(p))
                .expect("channel is full to handle messages");
            self.total_jobs += 1;
        }
    }
    pub fn close(&self) {
        let mut close_sent = 0;
        while let Some(ref ch_s) = self.ch_data {
            ch_s.send(Box::new(MultiDownloadParameters {
                ..Default::default()
            }))
            .expect("channel is full to handle messages");
            close_sent += 1;
            if close_sent == self.total_worker {
                thread::sleep(time::Duration::from_millis(1000));
                info!("request pool closed");
                return;
            }
        }
    }
    pub fn wait(mut self) -> Result<Vec<u8>, Error> {
        let mut results = Vec::<Result<MultiDownloadParameters, Error>>::new();
        self.ch_data.take();
        loop {
            thread::sleep(time::Duration::from_millis(1000));
            let result = self
                .ch_result
                .recv()
                .expect("channel is full to handle messages");

            results.push(result);
            info!("{} job excuted ", results.len());

            if results.len() == self.total_jobs {
                self.close();
                for res in results {
                    debug!("{:?}", res);
                }
                let inner = self.data.lock().unwrap();
                return Ok((&*inner).clone());
            }
        }
    }
}
