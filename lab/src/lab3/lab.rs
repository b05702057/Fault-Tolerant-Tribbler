// use crate::lab2::{bin_client::BinClient, trib_front::TribFront};
// use crate::lab3::keeper_server::KeeperServer;
// use tribbler::{config::KeeperConfig, err::TribResult, storage::BinStorage, trib::Server};

// use super::keeper_client;

// /// This function accepts a list of backend addresses, and returns a
// /// type which should implement the [BinStorage] trait to access the
// /// underlying storage system.
// #[allow(unused_variables)]
// pub async fn new_bin_client(backs: Vec<String>) -> TribResult<Box<dyn BinStorage>> {
//     Ok(Box::new(BinClient::new(backs)))
// }

// /// this async function accepts a [KeeperConfig] that should be used to start
// /// a new keeper server on the address given in the config.
// ///
// /// This function should block indefinitely and only return upon erroring. Make
// /// sure to send the proper signal to the channel in `kc` when the keeper has
// /// started.
// #[allow(unused_variables)]
// pub async fn serve_keeper(kc: KeeperConfig) -> TribResult<()> {
//     let keeper_server = KeeperServer::new(kc);
//     Ok(())

//     // let back_addrs = kc.backs.clone();
//     // println!("[DEBUG] lab2 serve_keeper: Config is: {:?}", kc);

//     // let mut keeper = KeeperServer::new(kc).await?;
//     // match keeper.serve().await {
//     //     Ok(_) => Ok(()),
//     //     Err(e) => {
//     //         println!("[DEBUG] lab2 serve_keeper error: {}", e.to_string());
//     //         Err(e)
//     //     }
//     // }
//     // Ok(keeper.serve().await?)
// }

// /// this function accepts a [BinStorage] client which should be used in order to
// /// implement the [Server] trait.
// ///
// /// You'll need to translate calls from the tribbler front-end into storage
// /// calls using the [BinStorage] interface.
// ///
// /// Additionally, two trait bounds [Send] and [Sync] are required of your
// /// implementation. This should guarantee your front-end is safe to use in the
// /// tribbler front-end service launched by the`trib-front` command
// #[allow(unused_variables)]
// pub async fn new_front(
//     bin_storage: Box<dyn BinStorage>,
// ) -> TribResult<Box<dyn Server + Send + Sync>> {
//     Ok(Box::new(TribFront::new(bin_storage)))
// }
