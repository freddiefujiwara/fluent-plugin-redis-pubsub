module Fluent
    class RedisPubsubOutput < BufferedOutput
        Plugin.register_output('redis_pubsub', self)
        attr_reader :host, :port, :channel, :redis

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
        end

        def shutdown
            @redis.quit
        end

        def format(tag, time, record)
            record['__tag__']  = tag
            record['__time__'] = time
            record.to_msgpack
        end

        def write(chunk)
            @redis.pipelined do
                chunk.msgpack_each do |record|
                    @redis.publish @channel, record.to_json
                end
            end
        end
    end
end
