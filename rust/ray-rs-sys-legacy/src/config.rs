
enum WorkerMode {
    HeadNodeDriver,
    RemoteDriver{
        redis_ip: String,
    }
    LocalDriver,
    Worker
}
