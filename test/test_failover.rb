ENV["RAILS_ENV"] = "test"

require 'rubygems'
require 'test/unit'
require 'mocha'
require 'redis_master_slave'

class FakeRedisClient
  attr_accessor :db
  attr_accessor :call
  attr_accessor :max_delayed_calls

  def initialize
    @hash = {0=>{}}
    @db = 0
    @max_delayed_calls = 100
    @call = 0
  end

  def get(key)
    @hash[@db][key]
  end

  def set(key, val)
    @hash[@db][key]=val
    "OK"
  end
  
  def select(i)
    @db=i
  end
end

class FailoverTest < Test::Unit::TestCase
  def test_master
    rms = RedisMasterSlave.new(FakeRedisClient.new,[FakeRedisClient.new,FakeRedisClient.new])

    # rms.acting_master.expects(:set).with("a",2).returns("OK")
    # rms.acting_master.expects(:get).with("a").returns(2)
    # rms.acting_master.expects(:set).with("a",3).returns("OK")
    # rms.acting_master.expects(:get).with("a").returns(3)
    assert_equal(nil, rms.get("a"))
    rms.set("a",2)
    assert_equal(2, rms.get("a"))
    rms.set("a",3)
    assert_equal(3, rms.get("a"))
  end
  
  def test_select
    rms = RedisMasterSlave.new(FakeRedisClient.new,[FakeRedisClient.new,FakeRedisClient.new])

    redises = [rms.acting_master, rms.master, rms.failover_slaves[0], rms.failover_slaves[1]]
    redises.each { |r|assert_equal(0, r.db) }
    rms.select(2)
    redises.each { |r|assert_equal(2, r.db) }
    rms.select(3)
    redises.each { |r|assert_equal(3, r.db) }
  end

  # def test_non_blocking_select
  #   slave0=FakeRedisClient.new

  #   def slave0.select(i) 
  #     puts "sleeping 15 seconds"
  #     sleep(15)
  #     @db=i
  #   end

  #   rms = RedisMasterSlave.new(FakeRedisClient.new,[slave0,FakeRedisClient.new])

  #   assert_raise Timeout::Error do
  #     rms.select(2)
  #   end
  #   assert_equal(2, rms.acting_master.db)
  #   assert_equal(2, rms.master.db)
  #   assert_equal(2, rms.failover_slaves[1].db)
  #   # assert_equal(0, rms.failover_slaves[0].db)

  #   # rms.select(3)
  # end
  
  def test_next_failover_slave
    rms = RedisMasterSlave.new(FakeRedisClient.new,[FakeRedisClient.new,FakeRedisClient.new])
    
    assert_equal(rms.next_failover_slave, rms.failover_slaves[0])
    assert_equal(rms.next_failover_slave, rms.failover_slaves[1])
    assert_equal(rms.next_failover_slave, rms.failover_slaves[0])
    assert_equal(rms.next_failover_slave, rms.failover_slaves[1])
  end
  
  def test_manual_failover
    rms = RedisMasterSlave.new(FakeRedisClient.new,[FakeRedisClient.new,FakeRedisClient.new])

    assert_equal(rms.acting_master, rms.master)
    rms.failover!
    assert_equal(rms.acting_master, rms.failover_slaves[0])
    rms.failover!
    assert_equal(rms.acting_master, rms.failover_slaves[1])
  end

  def test_simple_failover
    master=FakeRedisClient.new
    def master.set(key, val)
      puts "sleeping 10 seconds...."
      sleep(10)
      @hash[@db][key]=val
      "OK"
    end

    rms = RedisMasterSlave.new(master,[FakeRedisClient.new,FakeRedisClient.new])
    rms.redis_timeout=1
    rms.redis_retry_times=3

    assert_equal(nil, rms.get("a"))
    # assert_raise Timeout::Error do 
      rms.set("a",2)
    # end
    assert_equal(2, rms.get("a"))
    assert_equal(rms.acting_master, rms.failover_slaves[0])
  end

  def test_timeout_doesnt_failover
    master=FakeRedisClient.new
    def master.set(key, val)
      @call+=1
      puts "call:#{@call}; max:#{@max_delayed_calls}"
      if (@call<=@max_delayed_calls)
        puts "sleeping 15 seconds"
        sleep(15)
      end

      @hash[@db][key]=val
      "OK"
    end

    rms = RedisMasterSlave.new(master,[FakeRedisClient.new,FakeRedisClient.new])
    rms.redis_timeout=1
    rms.redis_retry_times=3

    # Testing if it fails once, then recovers.
    master.call=0
    master.max_delayed_calls=1
    assert_equal(nil, rms.get("a"))
    assert_raise RedisMasterSlave::FailoverEvent do
      rms.set("a",2)
    end
    assert_equal(2, rms.get("a"))
    assert_equal(rms.acting_master, master)

    # Testing if it fails 2 times in a row, then recovers.
    master.call=0
    master.max_delayed_calls=2
    assert_raise RedisMasterSlave::FailoverEvent do
      rms.set("a",3)
    end
    assert_equal(3, rms.get("a"))
    assert_equal(rms.acting_master, master)
  end

end
