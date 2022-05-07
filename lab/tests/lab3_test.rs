use std::{
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use lab::{self, lab1, lab2};
use log::LevelFilter;
use tokio::{sync::mpsc::Sender as MpscSender, task::JoinHandle};

use tribbler::addr::rand::rand_port;
#[allow(unused_imports)]
use tribbler::{
    self,
    config::{KeeperConfig, BackConfig},
    err::{TribResult, TribblerError},
    storage::{KeyList, KeyString, KeyValue, MemStorage, Pattern, Storage},
};

const DEFAULT_HOST: &str = "localhost:3000";

async fn setup_back(
    addr: Option<String>,
    storage: Option<Box<dyn Storage + Send + Sync>>,
) -> TribResult<(JoinHandle<TribResult<()>>, MpscSender<()>)> {
    let _ = env_logger::builder()
        .default_format()
        .filter_level(LevelFilter::Info)
        .try_init();
    let addr = match addr {
        Some(x) => x,
        None => DEFAULT_HOST.to_string(),
    };
    let storage = match storage {
        Some(x) => x,
        None => Box::new(MemStorage::new()),
    };
    let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
    let (shut_tx, shut_rx) = tokio::sync::mpsc::channel(1);
    let cfg = BackConfig {
        addr: addr.to_string(),
        storage: storage,
        ready: Some(tx.clone()),
        shutdown: Some(shut_rx),
    };

    let handle = spawn_back(cfg);
    let ready = rx.recv_timeout(Duration::from_secs(5))?;
    if !ready {
        return Err(Box::new(TribblerError::Unknown(
            "back failed to start".to_string(),
        )));
    }

    Ok((handle, shut_tx.clone()))
}

fn spawn_back(cfg: BackConfig) -> tokio::task::JoinHandle<TribResult<()>> {
    tokio::spawn(lab1::serve_back(cfg))
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: value.to_string(),
    }
}

fn pat(prefix: &str, suffix: &str) -> Pattern {
    Pattern {
        prefix: prefix.to_string(),
        suffix: suffix.to_string(),
    }
}


#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_2_backs_one_leave() -> TribResult<()> {
    let back_addrs = vec!["127.0.0.1:34404".to_string(), "127.0.0.1:34405".to_string()];
    let addr_back1 = Some(back_addrs[0].clone());
    let addr_back2 = Some(back_addrs[1].clone());
    
    let (_handle1, shut_tx1) = setup_back(addr_back1.clone(), None).await?;

    let (_handle2, shut_tx2) = setup_back(addr_back2, None).await?;

    let bc = lab2::new_bin_client(back_addrs).await?;
    let bin1 = bc.bin("bin1").await?;
    let bin2 = bc.bin("bin2").await?;

    // bin 1 ops
    let _ = bin1.list_append(&kv("t1", "v1")).await?;
    let _ = bin1.list_append(&kv("t2", "v2")).await?;
    let r = bin1.list_keys(&pat("", "")).await?.0;
    assert_eq!(2, r.len());

    // bin 2 ops
    let _ = bin2.list_append(&kv("t1", "v1")).await?;
    let _ = bin2.list_append(&kv("t2", "v2")).await?;
    let _ = bin2.list_append(&kv("t3", "v3")).await?;
    let r2 = bin2.list_keys(&pat("", "")).await?.0;
    assert_eq!(3, r2.len());

    // Shut down first back
    let _ = shut_tx1.send(()).await;

    // Ops should still work if 1 back crashes

    let r = bin1.list_keys(&pat("", "")).await?.0;
    assert_eq!(2, r.len());

    let r2 = bin2.list_keys(&pat("", "")).await?.0;
    assert_eq!(3, r2.len());

    Ok(())
}


// 2 backs start. Then back 1 leave, then join again, then back 2 leave.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_2_backs_leave_join_leave() -> TribResult<()> {
    let back_addrs = vec!["127.0.0.1:34404".to_string(), "127.0.0.1:34405".to_string()];
    let addr_back1 = Some(back_addrs[0].clone());
    let addr_back2 = Some(back_addrs[1].clone());
    
    let (_handle1, shut_tx1) = setup_back(addr_back1.clone(), None).await?;

    let (_handle2, shut_tx2) = setup_back(addr_back2, None).await?;


    // Setup Keeper
    let keeper_addrs = vec!["127.0.0.1:34704".to_string()];
    let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
    let (shut_tx, shut_rx) = tokio::sync::mpsc::channel(1);
    let cfg_keeper = KeeperConfig {
        backs: back_addrs.clone(),
        addrs: keeper_addrs,
        this: 0 as usize,
        id: 0 as u128,
        ready: Some(tx.clone()),
        shutdown: Some(shut_rx),
    };

    let keeper_handle = tokio::spawn(lab2::serve_keeper(cfg_keeper));
    let ready = match rx.recv_timeout(Duration::from_secs(12)) {
        Ok(ready) => ready,
        Err(_) => panic!("Timed out while starting keeper")

    };
    if !ready {
        panic!("Failed to start keeper");
    }

    let bc = lab2::new_bin_client(back_addrs).await?;
    let bin1 = bc.bin("bin1").await?;
    let bin2 = bc.bin("bin2").await?;

    // bin 1 ops
    let _ = bin1.list_append(&kv("t1", "v1")).await?;
    let _ = bin1.list_append(&kv("t2", "v2")).await?;
    let r = bin1.list_keys(&pat("", "")).await?.0;
    assert_eq!(2, r.len());

    // bin 2 ops
    let _ = bin2.list_append(&kv("t1", "v1")).await?;
    let _ = bin2.list_append(&kv("t2", "v2")).await?;
    let _ = bin2.list_append(&kv("t3", "v3")).await?;
    let r2 = bin2.list_keys(&pat("", "")).await?.0;
    assert_eq!(3, r2.len());

    
    // Shut down first back
    println!("IN TEST, SHUTTING DOWN BACK 1 (INDEX 0)");
    let _ = shut_tx1.send(()).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Ops should still work if 1 back crashes

    let r = bin1.list_keys(&pat("", "")).await?.0;
    assert_eq!(2, r.len());

    let r2 = bin2.list_keys(&pat("", "")).await?.0;
    assert_eq!(3, r2.len());

    // Sleep 30 secs
    tokio::time::sleep(Duration::from_secs(30)).await;
    
    println!("IN TEST, TURNING ON BACK 1 (INDEX 0)");
    // Bring server 1 back up again
    let (_handle1, shut_tx1) = setup_back(addr_back1.clone(), None).await?;

    // Ops should still work when server 1 is brough back up

    let r = bin1.list_keys(&pat("", "")).await?.0;
    assert_eq!(2, r.len());

    let r2 = bin2.list_keys(&pat("", "")).await?.0;
    assert_eq!(3, r2.len());

    // Sleep 30 secs
    tokio::time::sleep(Duration::from_secs(30)).await;

    // Shut down server 2
    println!("IN TEST, SHUTTING DOWN BACK 2 (INDEX 1)");
    let _ = shut_tx2.send(()).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Ops should still work
    let r = bin1.list_keys(&pat("", "")).await?.0;
    let r2 = bin2.list_keys(&pat("", "")).await?.0;
    println!("IN TEST, r len: {}, r2 len: {}", r.len(), r2.len());
    assert_eq!(2, r.len());
    assert_eq!(3, r2.len());

    Ok(())
}



// 3 backs start. Then back 1 leave, then back 2 leave, then back 1 join, then back 3 leave.
// Migration should allow us to get data in back 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_3_backs_migration() -> TribResult<()> {
    let back_addrs = vec!["127.0.0.1:34404".to_string(), "127.0.0.1:34405".to_string(), "127.0.0.1:34406".to_string()];
    let addr_back1 = Some(back_addrs[0].clone());
    let addr_back2 = Some(back_addrs[1].clone());
    let addr_back3 = Some(back_addrs[2].clone());
    
    let (_handle1, shut_tx1) = setup_back(addr_back1.clone(), None).await?;
    let (_handle2, shut_tx2) = setup_back(addr_back2.clone(), None).await?;
    let (_handle3, shut_tx3) = setup_back(addr_back3.clone(), None).await?;


    // Setup Keeper
    let keeper_addrs = vec!["127.0.0.1:34704".to_string()];
    let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
    let (shut_tx, shut_rx) = tokio::sync::mpsc::channel(1);
    let cfg_keeper = KeeperConfig {
        backs: back_addrs.clone(),
        addrs: keeper_addrs,
        this: 0 as usize,
        id: 0 as u128,
        ready: Some(tx.clone()),
        shutdown: Some(shut_rx),
    };

    let keeper_handle = tokio::spawn(lab2::serve_keeper(cfg_keeper));
    let ready = match rx.recv_timeout(Duration::from_secs(12)) {
        Ok(ready) => ready,
        Err(_) => panic!("Timed out while starting keeper")

    };
    if !ready {
        panic!("Failed to start keeper");
    }

    let bc = lab2::new_bin_client(back_addrs).await?;
    let bin1 = bc.bin("bin1").await?;
    let bin2 = bc.bin("bin2").await?;

    // bin 1 ops
    let _ = bin1.list_append(&kv("t1", "v1")).await?;
    let _ = bin1.list_append(&kv("t2", "v2")).await?;
    let r = bin1.list_keys(&pat("", "")).await?.0;
    assert_eq!(2, r.len());

    // bin 2 ops
    let _ = bin2.list_append(&kv("t1", "v1")).await?;
    let _ = bin2.list_append(&kv("t2", "v2")).await?;
    let _ = bin2.list_append(&kv("t3", "v3")).await?;
    let r2 = bin2.list_keys(&pat("", "")).await?.0;
    assert_eq!(3, r2.len());

    // Shut down first back
    let _ = shut_tx1.send(()).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Ops should still work if 1 back crashes

    let r = bin1.list_keys(&pat("", "")).await?.0;
    assert_eq!(2, r.len());

    let r2 = bin2.list_keys(&pat("", "")).await?.0;
    assert_eq!(3, r2.len());

    // Sleep 30 secs
    tokio::time::sleep(Duration::from_secs(30)).await;
    // Shut down server 2
    let _ = shut_tx2.send(()).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    // Ops should still work
    let r = bin1.list_keys(&pat("", "")).await?.0;
    assert_eq!(2, r.len());
    let r2 = bin2.list_keys(&pat("", "")).await?.0;
    assert_eq!(3, r2.len());


    // Sleep 30 secs
    tokio::time::sleep(Duration::from_secs(30)).await;
    
    // Bring server 1 back up again
    let (_handle1, shut_tx1) = setup_back(addr_back1.clone(), None).await?;
    // Ops should still work 
    let r = bin1.list_keys(&pat("", "")).await?.0;
    assert_eq!(2, r.len());
    let r2 = bin2.list_keys(&pat("", "")).await?.0;
    assert_eq!(3, r2.len());

    // Sleep 30 secs
    tokio::time::sleep(Duration::from_secs(30)).await;
    // Shut down server 3
    let _ = shut_tx3.send(()).await;
    // Ops should still work 
    let r = bin1.list_keys(&pat("", "")).await?.0;
    assert_eq!(2, r.len());
    let r2 = bin2.list_keys(&pat("", "")).await?.0;
    assert_eq!(3, r2.len());

    Ok(())
}

// 2 backs start. Then back 1 leave, then join again, then back 2 leave.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_2_backs_shutdown_second() -> TribResult<()> {
    let back_addrs = vec!["127.0.0.1:34404".to_string(), "127.0.0.1:34405".to_string()];
    let addr_back1 = Some(back_addrs[0].clone());
    let addr_back2 = Some(back_addrs[1].clone());
    
    let (_handle1, shut_tx1) = setup_back(addr_back1.clone(), None).await?;

    let (_handle2, shut_tx2) = setup_back(addr_back2, None).await?;


    // Setup Keeper
    let keeper_addrs = vec!["127.0.0.1:34704".to_string()];
    let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
    let (shut_tx, shut_rx) = tokio::sync::mpsc::channel(1);
    let cfg_keeper = KeeperConfig {
        backs: back_addrs.clone(),
        addrs: keeper_addrs,
        this: 0 as usize,
        id: 0 as u128,
        ready: Some(tx.clone()),
        shutdown: Some(shut_rx),
    };

    let keeper_handle = tokio::spawn(lab2::serve_keeper(cfg_keeper));
    let ready = match rx.recv_timeout(Duration::from_secs(12)) {
        Ok(ready) => ready,
        Err(_) => panic!("Timed out while starting keeper")

    };
    if !ready {
        panic!("Failed to start keeper");
    }

    let bc = lab2::new_bin_client(back_addrs).await?;
    let bin1 = bc.bin("bin1").await?;
    let bin2 = bc.bin("bin2").await?;

    // bin 1 ops
    let _ = bin1.list_append(&kv("t1", "v1")).await?;
    let _ = bin1.list_append(&kv("t2", "v2")).await?;
    let r = bin1.list_keys(&pat("", "")).await?.0;
    assert_eq!(2, r.len());

    // bin 2 ops
    let _ = bin2.list_append(&kv("t1", "v1")).await?;
    let _ = bin2.list_append(&kv("t2", "v2")).await?;
    let _ = bin2.list_append(&kv("t3", "v3")).await?;
    let r2 = bin2.list_keys(&pat("", "")).await?.0;
    assert_eq!(3, r2.len());

    
    // Shut down SECOND BACK
    println!("IN TEST, SHUTTING DOWN BACK 2 (INDEX 1)");
    let _ = shut_tx2.send(()).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Ops should still work if 1 back crashes

    let r = bin1.list_keys(&pat("", "")).await?.0;
    assert_eq!(2, r.len());

    let r2 = bin2.list_keys(&pat("", "")).await?.0;
    assert_eq!(3, r2.len());


    Ok(())
}


// 2 backs start. Then back 1 leave, then join again, then back 2 leave.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_4_backs_shutdown_last() -> TribResult<()> {
    let back_addrs = vec!["127.0.0.1:34404".to_string(), "127.0.0.1:34405".to_string(), "127.0.0.1:34406".to_string(), "127.0.0.1:34407".to_string()];
    let addr_back1 = Some(back_addrs[0].clone());
    let addr_back2 = Some(back_addrs[1].clone());
    let addr_back3 = Some(back_addrs[2].clone());
    let addr_back4 = Some(back_addrs[3].clone());
    
    let (_handle1, shut_tx1) = setup_back(addr_back1.clone(), None).await?;
    let (_handle2, shut_tx2) = setup_back(addr_back2.clone(), None).await?;
    let (_handle3, shut_tx3) = setup_back(addr_back3.clone(), None).await?;
    let (_handle4, shut_tx4) = setup_back(addr_back4.clone(), None).await?;


    // Setup Keeper
    let keeper_addrs = vec!["127.0.0.1:34704".to_string()];
    let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
    let (shut_tx, shut_rx) = tokio::sync::mpsc::channel(1);
    let cfg_keeper = KeeperConfig {
        backs: back_addrs.clone(),
        addrs: keeper_addrs,
        this: 0 as usize,
        id: 0 as u128,
        ready: Some(tx.clone()),
        shutdown: Some(shut_rx),
    };

    let keeper_handle = tokio::spawn(lab2::serve_keeper(cfg_keeper));
    let ready = match rx.recv_timeout(Duration::from_secs(12)) {
        Ok(ready) => ready,
        Err(_) => panic!("Timed out while starting keeper")

    };
    if !ready {
        panic!("Failed to start keeper");
    }

    let bc = lab2::new_bin_client(back_addrs).await?;
    let bin1 = bc.bin("bin1").await?;
    let bin2 = bc.bin("bin2").await?;

    // bin 1 ops
    let _ = bin1.list_append(&kv("t1", "v1")).await?;
    let _ = bin1.list_append(&kv("t2", "v2")).await?;
    let r = bin1.list_keys(&pat("", "")).await?.0;
    assert_eq!(2, r.len());

    // bin 2 ops
    let _ = bin2.list_append(&kv("t1", "v1")).await?;
    let _ = bin2.list_append(&kv("t2", "v2")).await?;
    let _ = bin2.list_append(&kv("t3", "v3")).await?;
    let r2 = bin2.list_keys(&pat("", "")).await?.0;
    assert_eq!(3, r2.len());

    
    // Shut down SECOND BACK
    println!("IN TEST, SHUTTING DOWN LAST BACK");
    let _ = shut_tx4.send(()).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Ops should still work if 1 back crashes

    let r = bin1.list_keys(&pat("", "")).await?.0;
    assert_eq!(2, r.len());

    let r2 = bin2.list_keys(&pat("", "")).await?.0;
    assert_eq!(3, r2.len());


    Ok(())
}
