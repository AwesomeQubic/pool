use async_std::{
    fs::{create_dir, File, OpenOptions},
    future,
    io::WriteExt,
    path::Path,
};
use crossbeam::queue::SegQueue;
use pool::{initialize, spawn, TaskHandle};

pub mod pool;

fn main() {
    initialize();
    let handle: TaskHandle<(), _> = spawn(greet());
    handle.join_blocking();
}

async fn greet() {
    println!("1");
    spawn(async {
        let path = Path::new("./output");
        let _ = create_dir(path).await;
    })
    .await
    .unwrap();

    let our_futures = (0..10)
        .map(|x| write(x))
        .map(|x| spawn(x))
        .collect::<Vec<_>>();

    for ele in our_futures {
        ele.await;
    }
}

async fn write(x: i32) {
    let path = Path::new("./output").join(Path::new(&format!("{x}.txt")));
    println!("{path:?}");
    let open = OpenOptions::new().create(true).write(true).open(path).await;
    if let Ok(mut file) = open {
        let _ = file.write(format!("I am {x}").as_bytes()).await;
    };
}
