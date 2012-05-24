require 'uri'
require 'timeout'
require 'redis/namespace'

module RedisMasterSlave
  class FailoverEvent < StandardError; end
  class PermanentFail < StandardError; end
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
      @failover_index    = 0
      @failover_slaves   = {}
      @timeout           = 15
      @retry_times       = 5
      @mode              = "failover"
      @current_db      ||= 0
      @namespace_rms   ||= nil
      @status            = "active"
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
    attr_accessor :timeout

    #
    # Number of times to timeout before redis will failover to slave
    # 
    attr_accessor :retry_times

    #
    # Current Database being used by active_master
    # 
    attr_accessor :current_db

    #
    # Mode for the master_slave to operate: "failover" or "dry_run"
    #
    attr_accessor :mode

    #
    # namespace_rms defined for the Redis::Namespace object.  Default = nil
    #
    attr_accessor :namespace_rms

    #
    # current state of the instance
    #
    attr_accessor :status

    # :type needs to go the the Redis instance
    undef_method :type

    #
    # Select a specific db for all redis masters and slaves
    #
    # TODO: This needs to be refactored with method_missing, as it's the same logic
    #
    def select(db)
      # Rails.logger.debug("RedisMasterSlave:select(#{db}) on acting_master: #{@acting_master}")
      i=0
      remaining_failovers = (@mode=="failover") ? @slave_configs.size - @failover_index : 0
      @failover_ctr=0

      begin
        Timeout.timeout(@timeout) do
          @acting_master.select(db)
        end
        @current_db=db
      rescue Timeout::Error, Errno::ECONNREFUSED => e
        # Rails.logger.debug("RedisMasterSlave: Error caught in select(#{db}) (i:#{i}; j:#{remaining_failovers})")
        if (i+=1)>=@retry_times
          i=0
          remaining_failovers-=1
          @failover_ctr+=1
          failover!(true)
        end
        
        # make sure we only failover if there's a slave
        if (remaining_failovers>=0)
          # Rails.logger.debug("RedisMasterSlave: retrying (i:#{i}; j:#{remaining_failovers})")
          retry
        end
      ensure
        raise RedisMasterSlave::FailoverEvent if @failover_ctr>0
      end
    end

    #
    # Failover to the next slave
    # 
    def failover!(skip_select = false)
      # Rails.logger.debug("RedisMasterSlave: failover!")
      if @mode=="failover"
        next_slave = next_failover_slave!
        @status = "failover"
        unless next_slave
          # Leaving this > 0 will trump the PermanentFail error with a Failover Event
          @failover_ctr=0
          @status = "permanent_fail"
          raise RedisMasterSlave::PermanentFail 
        end
        @acting_master = next_slave
        # Make sure to stay on same db as old master.
        @acting_master.select(@current_db) unless skip_select
        # raise RedisMasterSlave::FailoverEvent
      end
    end

    #
    # Return the next failover slave to use.  Initialized the redis instance if necessary.
    #
    def next_failover_slave!
      # Rails.logger.debug("RedisMasterSlave: next_failover_slave!: failover_index: #{@failover_index}")
      @failover_index=[@failover_index, @slave_configs.size].min + 1
      @failover_slaves[@failover_index-1] ||= make_client(@slave_configs[@failover_index-1])
    end

    # Specifically for transactions.  EXEC, DISCARD, UNWATCH and WATCH are handled as normal.
    def multi
      if !block_given?
        @acting_master.multi
      else
        @acting_master.multi{yield}
      end
    end

    def namespace=(ns=nil)
      @namespace_rms = ns
      @acting_master.namespace=ns
    end

    # 
    # TODO: make the method_missing memoize a class method
    # TODO: This needs to be refactored with seled method, as it's the same logic
    # 
    def method_missing(method, *params, &block) # :nodoc:
      # Rails.logger.debug("RedisMasterSlave:#{method}(#{params*', '})")
      if @acting_master.respond_to?(method)

        i=0
        remaining_failovers = (@mode=="failover") ? [@slave_configs.size - @failover_index,0].max : 0
        @failover_ctr=0

        begin
          Timeout.timeout(@timeout) do
            @acting_master.send(method, *params, &block)
          end

        rescue Timeout::Error, Errno::ECONNREFUSED
          # Rails.logger.debug("RedisMasterSlave: Error Caught in call #{method}(#{params}) (i:#{i}; j:#{j})")
          if (i+=1)>=@retry_times
            i=0
            remaining_failovers-=1
            @failover_ctr+=1
            failover!
          end
          
          # make sure we only failover if there's a slave
          if (remaining_failovers>=0)
            # Rails.logger.debug("RedisMasterSlave: retrying (i:#{i}; j:#{remaining_failovers})")
            retry
          end
        ensure
          raise RedisMasterSlave::FailoverEvent if @failover_ctr>0
        end
      else
        super
      end
    end

    # helpful for testing
    def throw_failover_event_exception
      raise RedisMasterSlave::FailoverEvent
    end

    def throw_general_exception
      raise Exception
    end

    def respond_to_with_redis?(symbol, include_private=false)
      respond_to_without_redis?(symbol, include_private) || 
        @acting_master.respond_to?(symbol, include_private)
    end
    alias_method :respond_to_without_redis?, :respond_to?
    alias_method :respond_to?, :respond_to_with_redis?

    private

    def make_client(config)
      # Rails.logger.debug("RedisMasterSlave: make_client(#{config.inspect})")
      case config
      when String
        # URL like redis://localhost:6379/db
        uri = URI.parse(config)
        db = uri.path.split("/")[1].to_i
        db ||= 0

        r = Redis.new(:host => uri.host, :port => uri.port, :db => db)
        Redis::Namespace.new(@namespace_rms, :redis => r)
      when Hash
        # Hash of Redis client options (string keys ok).
        redis_config = {}
        config.each do |key, value|
          redis_config[key.to_sym] = value
        end
        r = Redis.new(redis_config)
        @namespace_rms = config[:namespace]
        Redis::Namespace.new(@namespace_rms, :redis => r)
      else
        # Hopefully a client object.
        config
      end
    end
  end
end
