DEBUG=false

require 'rubygems'
require 'test/unit'
require 'mocha'
require 'redis_master_slave'

PORT    = 6379
OPTIONS_HASH = {:port => PORT, :db => 15, :timeout => 3}
OPTIONS_STR   = "redis://127.0.0.1:#{PORT}/15"
NODES   = ["redis://127.0.0.1:6380/15"]

class FakeRedisClient
  attr_accessor :name
  attr_accessor :db
  attr_accessor :call
  attr_accessor :max_delayed_calls
  attr_accessor :in_multi_block
  attr_accessor :queue
  attr_accessor :hash

  def initialize(name=nil)
    @name = name
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
    @hash[@db]={}
  end

  # TODO: Stub out MULTI and EXEC
end
