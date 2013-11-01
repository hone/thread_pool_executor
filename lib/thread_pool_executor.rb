class QueueError < StandardError; end

# based off of Java's ThreadPoolExecutor, http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ThreadPoolExecutor.html
class ThreadPoolExecutor
  SLEEP_TIME = 0.001
  POISON = :pill

  attr_reader :queue, :status, :manager_thread

  def initialize(queue, min_pool_size = 1, max_pool_size = 5, keepalive = 5, tick_time = 1)
    @min_pool_size  = min_pool_size
    @max_pool_size  = max_pool_size
    @queue          = queue
    @keepalive      = keepalive
    @tick_time      = tick_time
    @thread_pool    = []
    @status         = nil
    @manager_thread = start
  end

  def pool_size
    @thread_pool.size
  end

  def shutdown_now
    @status = :shutdown
  end

  def shutdown
    @status = :shutting_down
    1.upto(@thread_pool.size).each { @queue.push(POISON) }
  end

  def await_shutdown
    @thread_pool.each {|thread| thread.join }
    @thread_pool.delete_if {|thread| !thread.alive? }
  end

  def execute(job)
    raise QueueError.new("Can't execute jobs if executor is shutdown") if [:shutdown, :shutting_down].include?(@status)
    @queue.push(job)
  end

  def await_bootstrap
    loop do
      if @thread_pool.size == @min_pool_size
        break
      else
        sleep(SLEEP_TIME)
      end
    end
  end

  private
  def create_thread
    Thread.new {
      loop do
        break if @status == :shutdown

        job        = nil
        start_time = Time.now

        begin
          job = @queue.pop(true)
        rescue ThreadError => e
          if @status == :shutdown || (@thread_pool.size > @min_pool_size && start_time + @keepalive < Time.now)
            break
          else
            sleep(SLEEP_TIME)
            retry
          end
        end

        break if job == POISON
        job.call
      end
    }
  end

  def start
    @status = :running

    Thread.new {
      1.upto(@min_pool_size).each { @thread_pool << create_thread }
      loop do
        sleep(@tick_time)
        @thread_pool.delete_if {|thread| !thread.alive? }
        @thread_pool << create_thread if !@queue.size.zero? && @thread_pool.size < @max_pool_size && ![:shutdown, :shutting_down].include?(@status)
      end
    }
  end

end
