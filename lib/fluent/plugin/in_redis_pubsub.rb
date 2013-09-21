module Fluent
    class RedisPubsubInput < Input
        Plugin.register_input('redis_pubsub', self)

        attr_reader :host, :port, :channel, :redis
        config_param :host, :string, :default => 'localhost'
        config_param :port, :integer, :default =>  6379
        config_param :channel, :string
        config_param :tag, :string

        def initialize
            super
            require 'redis'
            require 'msgpack'
        end

        def configure(config)
            super
            @host    = config.has_key?('host')    ? config['host']         : 'localhost'
            @port    = config.has_key?('port')    ? config['port'].to_i    : 6379
            raise Fluent::ConfigError, "need channel" if not config.has_key?('channel') or config['channel'].empty?
            @channel = config['channel'].to_s
        end

        def start
            super
            @redis = Redis.new(:host => @host, :port => @port ,:thread_safe => true)
            @redis.subscribe @channel do |on|
                on.message do |channel,msg|
                    Engine.emit @tag, Engine.now, JSON.parse(msg)
                end
            end
        end

        def shutdown
            @redis.unsubscribe @channel
            @redis.quit
        end
    end
end
