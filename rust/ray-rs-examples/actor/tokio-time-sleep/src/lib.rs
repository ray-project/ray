#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}

pub async fn sleep(d: u64) {
    let r = tokio::runtime::Handle::current().spawn(async move {
        let h_ = tokio::runtime::Handle::current();
        eprintln!("Tryna sleep here");
        // tokio::time::sleep(std::time::Duration::from_millis(d)).await;
        eprintln!("Done sleeping");
    }).await;
    // eprintln!("We're good: {:?}", r);
}
