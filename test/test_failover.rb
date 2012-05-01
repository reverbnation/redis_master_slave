ENV["RAILS_ENV"] = "test"
DEBUG=false

require 'rubygems'
require 'test/unit'
require 'mocha'
require 'redis_master_slave'

PORT    = 6379
OPTIONS = {:port => PORT, :db => 15, :timeout => 3}
NODES   = ["redis://127.0.0.1:6380/15"]

class FakeRedisClient
  attr_accessor :db
  attr_accessor :call
  attr_accessor :max_delayed_calls
  attr_accessor :in_multi_block
  attr_accessor :queue

  def initialize
    @hash = {0=>{}}
    @db = 0
    @max_delayed_calls = 0
    @call = 0
    @in_multi_block=nil
    @queue=nil
  end

  def get(key)
    @hash[@db][key]
  end

  def set(key, val)
    @call+=1
    if (@call<=@max_delayed_calls)
      puts "sleeping #{@call}:#{@max_delayed_calls}" if DEBUG
      sleep(15)
    end
    @hash[@db][key]=val
    "OK"
  end
  
  def select(i)
    @db=i
  end

  # TODO: Stub out MULTI and EXEC
end

class FailoverTest < Test::Unit::TestCase
  def test_master
    rms = RedisMasterSlave.new(FakeRedisClient.new,[FakeRedisClient.new,FakeRedisClient.new])

    assert_equal(nil, rms.get("a"))
    rms.set("a",2)
    assert_equal(2, rms.get("a"))
    rms.set("a",3)
    assert_equal(3, rms.get("a"))
  end

  # This currently requires a real running redis instance.
  def test_master_multi_block
    rms = RedisMasterSlave.new(Redis.new(OPTIONS))

    assert_equal(rms.multi {rms.set("a",2);rms.get("a")}, 
                 ["OK", "2"])
    assert_equal(rms.get("a"),"2")
    assert_equal(rms.multi do
                   rms.set("b",3);
                   rms.get("b")
                   rms.get("a")
                   rms.set("a",4);
                 end, 
                 ["OK", "3", "2", "OK"])
    assert_equal(rms.get("a"),"4")
  end

  # TODO: add WATCH and UNWATCH tests.
  def test_master_multi
    rms = RedisMasterSlave.new(Redis.new(OPTIONS))

    # Make sure you can discard the transaction
    assert_equal(rms.set("a",1), "OK")
    assert_equal(rms.multi, "OK")
    assert_equal(rms.set("a",2), "QUEUED")
    assert_equal(rms.get("a"), "QUEUED")
    assert_equal(rms.discard, "OK")
    assert_equal(rms.get("a"), "1")

    # Make sure you can commit the transaction
    assert_equal(rms.multi, "OK")
    assert_equal(rms.set("a",2), "QUEUED")
    assert_equal(rms.get("a"), "QUEUED")
    assert_equal(rms.exec, ["OK", "2"])
    assert_equal(rms.get("a"), "2")

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
    master.max_delayed_calls=10

    rms = RedisMasterSlave.new(master,[FakeRedisClient.new,FakeRedisClient.new])
    rms.redis_timeout=1
    rms.redis_retry_times=3

    assert_equal(nil, rms.get("a"))
    assert_raise RedisMasterSlave::FailoverEvent do 
      rms.set("a",2)
    end
    assert_equal(2, rms.get("a"))
    assert_equal(rms.acting_master, rms.failover_slaves[0])
  end

  def test_timeout_doesnt_failover
    master=FakeRedisClient.new

    rms = RedisMasterSlave.new(master,[FakeRedisClient.new,FakeRedisClient.new])
    rms.redis_timeout=1
    rms.redis_retry_times=3

    # Testing if it fails once, then recovers.
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

  def test_multiple_failover
    master=FakeRedisClient.new
    slave0=FakeRedisClient.new
    slave1=FakeRedisClient.new

    rms = RedisMasterSlave.new(master,[slave0,slave1])
    rms.redis_timeout=1
    rms.redis_retry_times=3
    master.max_delayed_calls=100

    # Make sure it fails over to slave0 first
    assert_equal(nil, rms.get("a"))
    assert_raise RedisMasterSlave::FailoverEvent do 
      rms.set("a",2)
    end
    assert_equal(2, rms.get("a"))
    assert_equal(rms.acting_master, slave0)

    # Test second failover
    slave0.call=0
    slave0.max_delayed_calls=10

    assert_equal(2, rms.get("a"))
    assert_raise RedisMasterSlave::FailoverEvent do 
      rms.set("a",3)
    end
    assert_equal(3, rms.get("a"))
    assert_equal(rms.acting_master, slave1)

    # Test third failover returns to first
    # Not sure if that's the right functionality, so need to evaluate.
    # Not really our use-case, however.
    slave1.call=0
    slave1.max_delayed_calls=15
    slave0.call=0
    slave0.max_delayed_calls=0

    assert_equal(3, rms.get("a"))
    assert_raise RedisMasterSlave::FailoverEvent do 
      rms.set("a",4)
    end
    assert_equal(4, rms.get("a"))
    assert_equal(rms.acting_master, slave0)
  end    
end
