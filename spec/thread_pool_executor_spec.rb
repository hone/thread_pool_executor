require 'timeout'
require_relative 'spec_helper'
require 'thread_pool_executor'

Thread.abort_on_exception = true

class AtomicCounter
  def initialize
    @mutex   = Mutex.new
    @counter = 0
  end

  def increment
    @mutex.synchronize do
      @counter += 1
    end
  end

  def count
    @counter
  end
end

class CounterJob
  def initialize(counter)
    @counter = counter
  end

  def call
    @counter.increment
  end
end

class ResourceJob < CounterJob
  def initialize(counter, mutex, cv)
    @mutex = mutex
    @cv    = cv
    super(counter)
  end

  def call
    @mutex.synchronize do
      @cv.wait(@mutex)
    end
    super
  end

  def to_s
    "ResourceJob"
  end
end

class SecondResourceJob < CounterJob
  def initialize(counter, mutex, cv)
    @mutex = mutex
    @cv    = cv
    super(counter)
  end

  def call
    @mutex.synchronize do
      @cv.broadcast
    end
    super
  end

  def to_s
    "SecondResourceJob"
  end
end

class SleepJob < CounterJob
  def initialize(counter, sleep_time = 0.1)
    @sleep_time = sleep_time
    super(counter)
  end

  def call
    sleep(@sleep_time)
    super
  end
end

describe ThreadPoolExecutor do
  TEST_WAIT = 5

  let(:queue)       { Queue.new }
  let(:min_threads) { 1 }
  let(:max_threads) { 3 }
  let(:keepalive)   { 1 }
  let(:executor)    { ThreadPoolExecutor.new(queue, min_threads, max_threads, keepalive) }
  let(:counter)     { AtomicCounter.new }

  describe "#execute" do
    it "pushes a job on the queue" do
      executor.execute(CounterJob.new(counter))
      expect(executor.queue.size).to eq(1)
    end

    it "don't allow execution if pool has shutdown" do
      executor.shutdown
      expect { executor.execute(CounterJob.new(counter)) }.to raise_error(QueueError)
    end
  end

  describe "#pool_size" do
    let(:min_threads) { 5 }

    it "starts the minimum pool size" do
      executor.await_bootstrap
      expect(executor.pool_size).to eq(5)
    end
  end

  describe "#shutdown" do
    let(:min_threads) { 1 }

    it "shuts all the workers down" do
      executor.await_bootstrap
      executor.shutdown
      Timeout::timeout(TEST_WAIT) { executor.await_shutdown }
      expect(executor.pool_size).to eq(0)
    end

    it "completes all already queued work" do
      mutex = Mutex.new
      cv    = ConditionVariable.new

      executor.execute(CounterJob.new(counter))
      executor.execute(ResourceJob.new(counter, mutex, cv))
      executor.await_bootstrap
      executor.shutdown
      cv.broadcast
      Timeout::timeout(TEST_WAIT) { executor.await_shutdown }

      expect(executor.pool_size).to eq(0)
      expect(counter.count).to eq(2)
    end
  end

  describe "#shutdown_now" do
    let(:max_threads) { 1 }

    it "stops as soon as it can" do
      executor.await_bootstrap
      executor.execute(SleepJob.new(counter))
      executor.execute(SleepJob.new(counter))
      executor.shutdown_now
      Timeout::timeout(TEST_WAIT) { executor.await_shutdown }

      expect(counter.count).to eq(1)
    end
  end

  it "does work" do
    executor.await_bootstrap
    executor.execute(CounterJob.new(counter))
    executor.shutdown
    Timeout::timeout(TEST_WAIT) { executor.await_shutdown }

    expect(counter.count).to eq(1)
  end

  context "multiple threads" do
    let(:min_threads) { 2 }

    it "does work concurrently" do
      mutex = Mutex.new
      cv    = ConditionVariable.new

      executor.await_bootstrap
      executor.execute(ResourceJob.new(counter, mutex, cv))
      executor.execute(SecondResourceJob.new(counter, mutex, cv))
      executor.shutdown
      Timeout::timeout(TEST_WAIT) { executor.await_shutdown }

      expect(counter.count).to eq(2)
    end
  end
end
