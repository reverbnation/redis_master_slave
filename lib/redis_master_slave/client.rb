require 'uri'
require 'timeout'
module RedisMasterSlave
  #
  # Wrapper around a pair of Redis connections, one master and one
  # slave.
  #
  # Read requests are directed to the slave, others are sent to the
  # master.
  #
  class Client
    attr_accessor :redis_timeout
    attr_accessor :redis_retry_times
    #
    # Create a new client.
    #
    # +master+ and +slave+ may be URL strings, Redis client option
    # hashes, or Redis clients.
    #
    def initialize(*args)
      case args.size
      when 1
        # config = args.first[Rails.env]
        config = args.first[ENV["RAILS_ENV"]]
        raise ArgumentError, "Poorly formatted config.  Please include environment, master, and slave" if config.nil?
        master_config = config['master'] || config[:master]
        slave_configs = config['slaves'] || config[:slaves] || {}
      when 2
        master_config, slave_configs = *args
      else
        raise ArgumentError, "wrong number of arguments (#{args.size} for 1..2)"
      end

      @acting_master = @master = make_client(master_config) or
        extend ReadOnly
      @failover_slaves = slave_configs.map{|config| make_client(config)}
      @failover_index  = 0
      @redis_timeout=15
      @redis_retry_times=5
    end

    #
    # The master client.
    #
    attr_accessor :master

    #
    # The client who is acting as master (normally @master)
    #
    attr_accessor :acting_master

    #
    # The array of slave clients.
    #
    attr_accessor :failover_slaves

    #
    # Index of the slave to use for the next failover.
    #
    attr_accessor :failover_index

    #
    # Return the next failover slave to use.
    #
    def next_failover_slave
      slave = @failover_slaves[@failover_index]
      @failover_index = (@failover_index + 1) % @failover_slaves.size
      slave
    end

    #
    # Select a specific db for all redis masters and slaves
    #
    # TODO: make this non-blocking so that if one fails, they don't all fail.
    # 
    def select(db)
      @master.select(db) && 
        @failover_slaves.each{|s| s.select(db)}
    end

    #
    # Failover to the next slave
    # 
    def failover!
      @acting_master = next_failover_slave
    end

    # Send everything master.
    def method_missing(method, *params, &block) # :nodoc:
      # Rails.logger.debug("redis_master_slave:#{method}(#{params*', '})")
      puts("redis_master_slave:#{method}(#{params*', '})")
      if @acting_master.respond_to?(method)
        i,j=0,0
        begin
          Timeout.timeout(@redis_timeout) do
            puts "in timeout"
            @acting_master.send(method, *params)
          end
        rescue Timeout::Error
          puts "rescuing timeout #{i} #{j}"
          if (i+=1)>=@redis_retry_times
            failover!
            i=0
            j+=1
          end

          if (j<@failover_slaves.size)
            retry
          end
        ensure
          raise RedisMasterSlave::FailoverEvent if (i>0)
        end
      else
        super
      end
    end

    def respond_to_with_redis?(symbol, include_private=false)
      respond_to_without_redis?(symbol, include_private) || 
        @acting_master.respond_to?(symbol, include_private)
    end
    alias_method :respond_to_without_redis?, :respond_to?
    alias_method :respond_to?, :respond_to_with_redis?

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
