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
      @slaves = slave_configs.map{|config| make_client(config)}
      @index  = 0
    end

    #
    # The master client.
    #
    attr_accessor :master

    #
    # The slave client.
    #
    attr_accessor :slaves

    #
    # Index of the slave to use for the next read.
    #
    attr_accessor :index

    #
    # Return the next read slave to use.
    #
    # Each call returns the following slave in sequence.
    #
    def next_slave
      slave = slaves[index]
      @index = (index + 1) % slaves.size
      slave
    end

    #
    # Select a specific db for all redis masters and slaves
    # 
    def select(db)
      @master.select(db) && @slaves.each{|s| s.select(db)}
    end

    class << self
      private

      def send_to_slave(command)
        class_eval <<-EOS
          def #{command}(*args, &block)
            next_slave.#{command}(*args, &block)
          end
        EOS
      end
    end

    send_to_slave :dbsize
    send_to_slave :exists
    send_to_slave :get
    send_to_slave :getbit
    send_to_slave :getrange
    send_to_slave :hexists
    send_to_slave :hget
    send_to_slave :hgetall
    send_to_slave :hkeys
    send_to_slave :hlen
    send_to_slave :hmget
    send_to_slave :hvals
    send_to_slave :keys
    send_to_slave :lindex
    send_to_slave :llen
    send_to_slave :lrange
    send_to_slave :mget
    send_to_slave :randomkey
    send_to_slave :scard
    send_to_slave :sdiff
    send_to_slave :sinter
    send_to_slave :sismember
    send_to_slave :smembers
    send_to_slave :sort
    send_to_slave :srandmember
    send_to_slave :strlen
    send_to_slave :sunion
    send_to_slave :ttl
    send_to_slave :type
    send_to_slave :zcard
    send_to_slave :zcount
    send_to_slave :zrange
    send_to_slave :zrangebyscore
    send_to_slave :zrank
    send_to_slave :zrevrange
    send_to_slave :zscore

    # Send everything else to master.
    def method_missing(method, *params, &block) # :nodoc:
      Rails.logger.debug("redis_master_slave:#{method}(#{params*', '})")
      if @master.respond_to?(method)
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
