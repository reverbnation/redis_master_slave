require 'uri'

module RedisMasterSlave
  #
  # Wrapper around a pair of Redis connections, one master and one
  # slave.
  #
  # Read requests are directed to the slave, others are sent to the
  # master.
  #
  class Client
    REDIS_SLAVE_METHODS = [:dbsize, :exists, :get, :getbit, :getrange, 
                           :hexists, :hget, :hgetall, :hkeys, :hlen, :hmget, 
                           :hvals, :keys, :lindex, :llen, :lrange, :mget, 
                           :randomkey, :scard, :sdiff, :sinter, :sismember, 
                           :smembers, :sort, :srandmember, :strlen, :sunion, 
                           :ttl, :type, :zcard, :zcount, :zrange, :zrangebyscore, 
                           :zrank, :zrevrange, :zscore]    
    #
    # Create a new client.
    #
    # +master+ and +slave+ may be URL strings, Redis client option
    # hashes, or Redis clients.
    #
    def initialize(*args)
      case args.size
      when 1
        config = args.first[Rails.env]
        raise ArgumentError, "Poorly formatted config.  Please include environment, master, and slave" unless config.present?
        master_config = config['master'] || config[:master]
        slave_configs = config['slaves'] || config[:slaves] || {}
      when 2
        master_config, slave_configs = *args
      else
        raise ArgumentError, "wrong number of arguments (#{args.size} for 1..2)"
      end

      @master = make_client(master_config) or
        extend ReadOnly
      @failover_slaves = slave_configs.map{|config| make_client(config)}
      @lb_slaves = slave_configs.select{|config| config["in_lb"]}.map{|config| make_client(config) }
      @failover_index  = 0
      @lb_index  = 0

    end

    #
    # The master client.
    #
    attr_accessor :master

    #
    # The slave client.
    #
    attr_accessor :failover_slaves

    #
    # The slave client.
    #
    attr_accessor :lb_slaves

    #
    # Index of the slave to use for the next read.
    #
    attr_accessor :lb_index

    #
    # Index of the slave to use for the next failover.
    #
    attr_accessor :failover_index

    #
    # Return the next failover slave to use.
    #
    # Each call returns the following slave in sequence.
    #
    def next_failover_slave
      slave = failover_slaves[@failover_index]
      @failover_index = (@failover_index + 1) % slaves.size
      slave
    end

    #
    # Return the next slave to use for a read operation
    #
    # Each call returns the following slave in sequence.
    #
    def next_lb_slave
      slave = lb_slaves[@lb_index]
      @lb_index = (@lb_index + 1) % lb_slaves.size
      slave
    end

    #
    # Select a specific db for all redis masters and slaves
    # 
    def select(db)
      @master.select(db) && 
        @failover_slaves.each{|s| s.select(db)} &&
        @lb_slaves.each{|s| s.select(db)}
    end

    # Send everything else to master.
    def method_missing(method, *params, &block) # :nodoc:
      Rails.logger.debug("redis_master_slave:#{method}(#{params*', '})")
      if (@lb_slaves.size > 0) && REDIS_SLAVE_METHODS.include?(method)
        next_lb_slave.send(method)
      elsif @master.respond_to?(method)
        @master.send(method, *params)
      else
        super
      end
    end

    def respond_to_with_redis?(symbol, include_private=false)
      respond_to_without_redis?(symbol, include_private) || 
        @master.respond_to?(symbol, include_private)
    end
    alias_method_chain :respond_to?, :redis

    private

    def make_client(config)
      case config
      when String
        # URL like redis://localhost:6379.
        uri = URI.parse(config)
        Redis.new(:host => uri.host, :port => uri.port)
      when Hash
        # Hash of Redis client options (string keys ok).
        redis_config = {}
        config.each do |key, value|
          redis_config[key.to_sym] = value
        end
        Redis.new(redis_config)
      else
        # Hopefully a client object.
        config
      end
    end
  end
end
