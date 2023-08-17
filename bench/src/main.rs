use clap::Parser;
use hyper::{body::HttpBody, Method, Request, StatusCode};
use rand::{Rng, RngCore};
use std::fmt;
use tokio::sync::broadcast;
use tracing::debug;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Config {
    #[arg(long, value_name = "URL")]
    server: String,

    #[arg(long, default_value_t = 128)]
    min_data_size: usize,

    #[arg(long, default_value_t = 262144)]
    max_data_size: usize,

    #[arg(long, default_value_t = 0.5)]
    get_ratio: f64,

    #[arg(long, default_value_t = 5.0)]
    warm_up_secs: f64,

    #[arg(long, default_value_t = 1)]
    num_users: usize,

    #[arg(long, default_value_t = 0.1)]
    access_ratio_secs: f64,

    #[arg(long, default_value_t = 10.0)]
    duration_secs: f64,

    #[arg(long, default_value_t = 10)]
    user_buffer_length: usize,
}

#[derive(Debug, Clone)]
enum ClientCommand {
    Start,
    End,
}

#[derive(Default)]
struct Stat {
    buf: Vec<f64>,
}

impl Stat {
    fn new(duration: f64) -> Self {
        Stat {
            buf: vec![duration],
        }
    }
    fn accum(&mut self, s: Stat) {
        self.buf.extend(s.buf.into_iter());
    }
    fn min(&self) -> f64 {
        self.buf.iter().fold(1.0e100, |a, b| f64::min(a, *b))
    }
    fn max(&self) -> f64 {
        self.buf.iter().fold(0.0, |a, b| f64::max(a, *b))
    }

    fn avg(&self) -> f64 {
        self.buf.iter().fold(0.0, |a, b| a + b) / (self.buf.len() as f64)
    }
    fn std_dev(&self) -> f64 {
        let avg = self.avg();
        self.buf
            .iter()
            .fold(0.0, |a, b| a + ((b - avg) * (b - avg)))
            / (self.buf.len() as f64)
    }
}

impl fmt::Display for Stat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "avg: {:10.3}, std_dev: {:10.3}, [min, max]=[{:10.3}, {:10.3}]",
            self.avg() * 1000.0,
            self.std_dev() * 1000.0,
            self.min() * 1000.0,
            self.max() * 1000.0
        )
    }
}

#[derive(Default)]
struct BenchResult {
    get_response_time: Stat,
    put_response_time: Stat,
    num_hit: usize,
    num_miss: usize,
    num_error: usize,
    sent_bytes: usize,
    recv_bytes: usize,
}

impl BenchResult {
    fn hit(duration: f64, nbytes: usize) -> Self {
        let mut s = BenchResult::default();
        s.get_response_time = Stat::new(duration);
        s.num_hit = 1;
        s.recv_bytes += nbytes;
        s
    }
    fn miss(duration: f64) -> Self {
        let mut s = BenchResult::default();
        s.get_response_time = Stat::new(duration);
        s.num_miss = 1;
        s
    }
    fn put(duration: f64, nbytes: usize) -> Self {
        let mut s = BenchResult::default();
        s.put_response_time = Stat::new(duration);
        s.sent_bytes += nbytes;
        s
    }
    fn error() -> Self {
        let mut s = BenchResult::default();
        s.num_error = 1;
        s
    }

    fn accum(&mut self, r: BenchResult) {
        self.get_response_time.accum(r.get_response_time);
        self.put_response_time.accum(r.put_response_time);
        self.num_hit += r.num_hit;
        self.num_miss += r.num_miss;
        self.num_error += r.num_error;
        self.sent_bytes += r.sent_bytes;
        self.recv_bytes += r.recv_bytes;
    }
}

impl fmt::Display for BenchResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "get: ")?;
        self.get_response_time.fmt(f)?;
        write!(f, "\nput: ")?;
        self.put_response_time.fmt(f)?;
        let total_access = (self.num_hit + self.num_miss + self.num_error) as f64;
        write!(
            f,
            "\ntotal: {:8}, hit: {:10.3}, miss: {:10.3}, error: {:10.3}",
            total_access,
            (self.num_hit as f64) / total_access,
            (self.num_miss as f64) / total_access,
            (self.num_error as f64) / total_access
        )?;
        write!(
            f,
            "\nsent: {:8.3}, recv: {:8.3} [M]",
            (self.sent_bytes as f64) / (1024.0 * 1024.0),
            (self.recv_bytes as f64) / (1024.0 * 1024.0),
        )
    }
}

#[derive(Clone)]
struct Data {
    size: usize,
    uri: String,
    sha256: String,
}

struct Client {
    config: Config,
    buffer: Vec<Data>,
    bench: BenchResult,
    http_client: hyper::Client<hyper::client::HttpConnector, hyper::Body>,
}

impl Client {
    async fn get(&mut self) -> BenchResult {
        let index = rand::random::<usize>() % self.buffer.len();
        let key = &self.buffer[index];

        let start_time = std::time::Instant::now();
        let mut resp = match self.http_client.get(key.uri.clone().parse().unwrap()).await {
            Ok(v) => v,
            Err(_e) => return BenchResult::error(),
        };

        let mut buf = Vec::new();
        while let Some(chunk) = resp.body_mut().data().await {
            let chunk = chunk.unwrap();
            buf.extend(chunk.to_vec());
        }
        let nbytes = buf.len();
        let duration = std::time::Instant::now() - start_time;
        let eq = key.size == buf.len() && key.sha256 == sha256::digest(buf);

        if eq && resp.status() == StatusCode::OK {
            BenchResult::hit(duration.as_secs_f64(), nbytes)
        } else {
            self.buffer = self
                .buffer
                .iter()
                .enumerate()
                .filter(|(i, _data)| (*i) != index)
                .map(|(_, d)| d.clone())
                .collect();
            BenchResult::miss(duration.as_secs_f64())
        }
    }

    fn generate_bytes(&mut self) -> Vec<u8> {
        let mut randgen = rand::thread_rng();
        let mut bytes =
            vec![0; randgen.gen_range(self.config.min_data_size..self.config.max_data_size)];
        randgen.fill_bytes(&mut bytes);
        bytes
    }

    async fn put(&mut self) -> BenchResult {
        let key = uuid::Uuid::new_v4();
        let uri = url::Url::parse(&self.config.server)
            .unwrap()
            .join(&format!("{}", key))
            .unwrap()
            .as_str()
            .to_string();
        let bytes = self.generate_bytes();
        let size = bytes.len();
        let sha256 = sha256::digest(&bytes);

        let start_time = std::time::Instant::now();
        let resp = match self
            .http_client
            .request(
                Request::builder()
                    .uri(uri.clone())
                    .method(Method::PUT)
                    .body(hyper::Body::from(bytes))
                    .unwrap(),
            )
            .await
        {
            Ok(v) => v,
            Err(_e) => return BenchResult::error(),
        };
        let duration = std::time::Instant::now() - start_time;

        if resp.status() != StatusCode::OK {
            return BenchResult::error();
        }
        self.buffer.push(Data { uri, sha256, size });
        if self.buffer.len() >= self.config.user_buffer_length {
            self.buffer = self.buffer[1..].iter().cloned().collect();
        }

        BenchResult::put(duration.as_secs_f64(), size)
    }

    async fn exec(&mut self) -> BenchResult {
        if self.buffer.len() == 0 || rand::random::<f64>() > self.config.get_ratio {
            self.put().await
        } else {
            self.get().await
        }
    }

    async fn iter(&mut self, profile: bool) {
        let start = std::time::Instant::now();
        let r = self.exec().await;
        if profile {
            self.bench.accum(r)
        }
        let end = std::time::Instant::now();

        let wait_time =
            start + std::time::Duration::from_secs_f64(self.config.access_ratio_secs) - end;
        if wait_time.as_secs() > 0 {
            tokio::time::sleep(wait_time).await
        }
    }

    async fn run(config: Config, mut rx: broadcast::Receiver<ClientCommand>) -> BenchResult {
        let mut client = Client {
            config,
            buffer: Vec::new(),
            bench: BenchResult::default(),
            http_client: hyper::Client::new(),
        };
        loop {
            tokio::select! {
                _ = client.iter(false) => {
                }
                _ = rx.recv() => {
                    break
                }
            };
        }
        loop {
            tokio::select! {
                _ = client.iter(true) => {
                }
                _ = rx.recv() => {
                    break
                }
            };
        }
        client.bench
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::Layer::new()
                    .with_ansi(true)
                    .with_file(true)
                    .with_line_number(true)
                    .with_level(true), //.json(),
            )
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .try_init()?;
    }

    let config = Config::parse();
    let (tx, _) = broadcast::channel(16);
    let mut users = Vec::new();

    for _ in 0..config.num_users {
        let config = config.clone();
        let rx = tx.subscribe();
        users.push(tokio::spawn(Client::run(config, rx)));
    }

    debug!("warm up");
    tokio::time::sleep(std::time::Duration::from_secs_f64(config.warm_up_secs)).await;

    debug!("profile start");
    tx.send(ClientCommand::Start)?;
    tokio::time::sleep(std::time::Duration::from_secs_f64(config.duration_secs)).await;
    tx.send(ClientCommand::End)?;
    debug!("profile end");

    let mut bench = BenchResult::default();
    for u in users {
        bench.accum(u.await?);
    }

    println!("{:?}", config);
    println!("{}", bench);

    Ok(())
}
