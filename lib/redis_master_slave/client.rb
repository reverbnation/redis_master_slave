require 'uri'
require 'timeout'
module RedisMasterSlave
  class FailoverEvent < StandardError; end
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
      @master_config, @slave_configs = *args
      raise ArgumentError, "wrong number of arguments (#{args.size} for 1..2)" if args.size > 2

      @acting_master = @master = make_client(@master_config) or
        extend ReadOnly
      @slave_configs ||= {}
      @failover_index  = 0
      @failover_slaves = {}
      @redis_timeout=15
      @redis_retry_times=5
      @mode="failover"
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
    # Amount of time before redis will timeout
    # 
    attr_accessor :redis_timeout

    #
    # Number of times to timeout before redis will failover to slave
    # 
    attr_accessor :redis_retry_times

    #
    # Current Database being used by active_master
    # 
    attr_accessor :current_db

    #
    # Mode for the master_slave to operate: "failover" or "dry_run"
    attr_accessor :mode

    #
    # Select a specific db for all redis masters and slaves
    #
    # TODO: make this non-blocking so that if one fails, they don't all fail.
    # 
    def select(db)
      @current_db=db
      @master.select(db)
    end

    #
    # Failover to the next slave
    # 
    def failover!
      if @mode=="failover"
        @acting_master = next_failover_slave
        # Make sure to stay on same db as old master.
        @acting_master.select(@current_db)
      end
    end

    #
    # Return the next failover slave to use.  Initialized the redis instance if necessary.
    #
    def next_failover_slave
      slave = @failover_slaves[@failover_index] ||= make_client(@slave_configs[@failover_index])
      @failover_index = (@failover_index + 1) % @slave_configs.size
      slave
    end

    # Specifically for transactions.  EXEC, DISCARD, UNWATCH and WATCH are handled as normal.
    def multi
      if !block_given?
        @acting_master.multi
      else
        @acting_master.multi{yield}
      end
    end

    # This works, but is ugly.
    # TODO: make the method_missing memoize a class method
    def method_missing(method, *params, &block) # :nodoc:
      # puts("redis_master_slave:#{method}(#{params*', '})")
      if @acting_master.respond_to?(method)
        i,j=0,0
        begin
          Timeout.timeout(@redis_timeout) do
            @acting_master.send(method, *params, &block)
          end
        rescue Timeout::Error
          if (i+=1)>=@redis_retry_times
            failover!
            i=0
            j+=1
          end
          
          # make sure we only failover if there's a slave
          if (j<@slave_configs.size)
            retry
          end
        ensure
          raise RedisMasterSlave::FailoverEvent if (i>0) || (j>0)
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
      # puts "make_client(config) = #{config}"
      raise ArgumentError, "Poorly formatted config argument.  Please include environment, master, and slave" if config.nil?
      case config
      when String
        # URL like redis://localhost:6379.
        uri = URI.parse(config)
        Redis.new(:host => uri.host, :port => uri.port)
      when Hash
        # Hash of Redis client options (string keys ok).
        redis_config = {}
        config[ENV["RAILS_ENV"]].each do |key, value|
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
