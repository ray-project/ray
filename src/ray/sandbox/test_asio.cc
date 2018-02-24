#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <iostream>
#include <queue>
#include <random>
#include <sys/time.h>
#include <chrono>
#include <boost/date_time/posix_time/posix_time.hpp>


long GetTime(void) {
  struct timeval  tv;
  gettimeofday(&tv, NULL);
  return tv.tv_usec/1000 + tv.tv_sec*1000;
}


// generate and enqueue max_num_tasks tasks according to an Poison arrival
// process with average inter arrival time mean_interarrival_interval
class TaskQueue {
  private:
    boost::asio::deadline_timer timer_;
    std::queue<long> task_queue_;
    int mean_interarrival_interval_;
    int max_num_tasks_;
    int num_tasks_;
    bool experiment_done_;
    std::default_random_engine generator_;
    std::exponential_distribution<double> exp_dist_;

  public:
    TaskQueue(boost::asio::io_service& io, int mean_interarrival_interval, int max_num_tasks)
    : timer_(io, boost::posix_time::milliseconds(mean_interarrival_interval)) {
      mean_interarrival_interval_ = mean_interarrival_interval;
      max_num_tasks_ = max_num_tasks;
      num_tasks_ = 0;
      experiment_done_ = false;
      // initialize a generator with a ranomd seed and construct
      // exponential_distribution object
      int seed = std::chrono::system_clock::now().time_since_epoch().count();
      generator_ = std::default_random_engine(seed);
      exp_dist_ = std::exponential_distribution<double>(1./(double)mean_interarrival_interval_);
      timer_.async_wait(boost::bind(&TaskQueue::Enqueue, this, boost::asio::placeholders::error));
    }

    ~TaskQueue() {
      ;
    }

    void Enqueue(const boost::system::error_code& error) {
      if (num_tasks_ < max_num_tasks_) {
        num_tasks_++;
        long now = GetTime();
        task_queue_.push(now);
        double interval = exp_dist_(generator_);
        interval = (interval > 1. ? interval : 1.);
        timer_.expires_at(timer_.expires_at() + boost::posix_time::milliseconds((int)interval));
        timer_.async_wait(boost::bind(&TaskQueue::Enqueue, this, boost::asio::placeholders::error));
      } else {
        experiment_done_ = true;
      }
    }

    long Dequeue(void) {
      if (task_queue_.size() == 0) {
        return -1;
      } else {
        long enqueued = task_queue_.front();
        long now = GetTime();
        task_queue_.pop();
        long delta = now - enqueued;
        return delta;
      }
    }

    bool isDone(void) {
      return experiment_done_;
    }
};

// server processing tasks; service time is exponentially distributed
class Server {
  private:
    boost::asio::deadline_timer timer_;
    TaskQueue *task_queue_;
    int mean_service_time_;
    std::default_random_engine generator_;
    std::exponential_distribution<double> exp_dist_;
    int timeouts_cnt_;
    int retry_timeout_;
    int num_scheduled_tasks_;

  public:
    Server(boost::asio::io_service& io, TaskQueue *task_queue, int mean_service_time, int timeout)
    : timer_(io, boost::posix_time::milliseconds(1)) {
      task_queue_ = task_queue;
      mean_service_time_ = mean_service_time;
      timeouts_cnt_ = 0;
      retry_timeout_ = timeout;
      int seed = std::chrono::system_clock::now().time_since_epoch().count();
      generator_ = std::default_random_engine(seed);
      exp_dist_ = std::exponential_distribution<double>(1./(double)mean_service_time_);
      timer_.async_wait(boost::bind(&Server::Start, this, boost::asio::placeholders::error));
    }

    ~Server() {
      std::cout << "timeouts_cnt_ = " << timeouts_cnt_ << std::endl;
      std::cout << "num_scheduled_tasks_ = " << num_scheduled_tasks_ << std::endl;
      std::cout << "overhead = " << (double)timeouts_cnt_/(double)num_scheduled_tasks_ << std::endl;
    }

    void Start(const boost::system::error_code& error) {
      if (task_queue_->isDone() == true) {
        ;
      } else {
        long wait_time = task_queue_->Dequeue();
        if (wait_time >= 0) {
          num_scheduled_tasks_++;
          double service_time = exp_dist_(generator_);
          timeouts_cnt_+= (wait_time + service_time) / retry_timeout_;
          service_time = (service_time > 1. ? service_time : 1.);
          timer_.expires_at(timer_.expires_at() + boost::posix_time::milliseconds((int)service_time));
          timer_.async_wait(boost::bind(&Server::Start, this, boost::asio::placeholders::error));
        } else {
          timer_.expires_at(timer_.expires_at() + boost::posix_time::milliseconds(1));
          timer_.async_wait(boost::bind(&Server::Start, this, boost::asio::placeholders::error));
        }
      }
    }
};


int main()  
{
  boost::asio::io_service io_service;

  int mean_interarrival_interval = 120; // ms
  int mean_service_time = 100; // ms
  int retry_timeout = 100; // ms
  int num_tasks = 1000; // total tasks being scheduled

  std::cout << "rate (tasks/sec) = " << 1000./(double)mean_interarrival_interval  << std::endl;
  std::cout << "utilization = " << (double)mean_service_time/(double)mean_interarrival_interval  << std::endl;

  TaskQueue task_queue(io_service, mean_interarrival_interval, num_tasks);
  Server server(io_service, &task_queue, mean_service_time, retry_timeout);

  io_service.run();

  return 0;
}
