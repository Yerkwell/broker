use std::sync::{Arc, Mutex};
use actix_web::{web, get, put, App, HttpServer, HttpResponse, Responder};
use std::collections::{VecDeque, HashMap};
use futures::StreamExt;
use lazy_static::lazy_static;
use serde::Deserialize;

type Storage = Arc<Mutex<HashMap<String, VecDeque<Vec<u8>>>>>;

lazy_static!{
    static ref MAX_SIZE: usize = std::env::var("MAX_SIZE").ok().and_then(|v| v.parse().ok()).unwrap_or(10);
}

#[put("/{queue}")]
async fn input(web::Path(queue): web::Path<String>, data: web::Data<Storage>, mut payload: web::Payload) -> impl Responder {
    let hm = &mut *(data.lock().unwrap());
    if !hm.contains_key(&queue) {
        hm.insert(queue.clone(), VecDeque::new());
    };
    if hm[&queue].len() >= *MAX_SIZE {
        return HttpResponse::TooManyRequests().body("")
    }
    let mut body = web::BytesMut::new();
    while let Some(chunk) = payload.next().await {
        body.extend_from_slice(&chunk.unwrap());
    }
    hm.get_mut(&queue).unwrap().push_back(body.to_vec());
    HttpResponse::Ok().body("Ok!")
}

#[get("/{queue}")]
async fn output(web::Path(queue): web::Path<String>, data: web::Data<Storage>, web::Query(qu): web::Query<QData>) -> impl Responder {
    let mut hm = data.lock().unwrap();
    let mut waited = 0;
    loop {
        match hm.get_mut(&queue) {
            Some(q) if q.len() > 0 => return HttpResponse::Ok().body(q.pop_front().unwrap()),
            _ if waited < qu.timeout.unwrap_or(0) => {
                actix_rt::time::delay_for(std::time::Duration::from_secs(1)).await;
                waited+=1;
            },
            _ => {
                return HttpResponse::NotFound().body("Not found")
            },
        }
    }
}

#[actix_web::main]
async fn start_api() -> std::io::Result<()> {
    let queue = Arc::new(Mutex::new(HashMap::<String, VecDeque<Vec<u8>>>::new()));
    HttpServer::new(move || {
        App::new()
            .data(queue.clone())
            .service(input)
            .service(output)
    })
        .bind("127.0.0.1:10101")?
        .run()
        .await
}

fn main() {
    println!("Hello, world!");
    start_api().unwrap();
}

#[derive(Deserialize)]
pub struct QData {
    pub timeout: Option<i32>
}
