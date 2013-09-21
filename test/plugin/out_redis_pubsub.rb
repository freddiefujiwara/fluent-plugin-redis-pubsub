require 'fluent/test'
require 'redis'
require 'fluent/plugin/out_redis_pubsub'

class FileOutputTest < Test::Unit::TestCase
    def setup
        Fluent::Test.setup

        @d = create_driver %[
            host    localhost
            port    6379
            channel test.channel
        ]
        @time = Time.now.to_i
    end

    def create_driver(config = CONFIG)
        Fluent::Test::BufferedOutputTestDriver.new(Fluent::RedisPubsubOutput).configure(config)
    end

    def test_configure
        assert_equal 'localhost'   , @d.instance.host
        assert_equal 6379          , @d.instance.port
        assert_equal 'test.channel', @d.instance.channel
        assert_raise Fluent::ConfigError do
            create_driver %[
                host    localhost
                port    6379
            ]
            create_driver %[
                host    localhost
                port    6379
                channel
            ]
        end
    end

    def test_format
        @d.emit({"a"=>1}, @time)
        @d.expect_format({"a"=>1,"__tag__" => 'test',"__time__" => @time}.to_msgpack)
        @d.run
    end

    def test_write
        redis = Redis.new
        redis.subscribe @d.instance.channel do |on|
            on.subscribe do
                @d.emit({"a"=>2}, @time)
                @d.run
            end
            on.message do |channel,msg|
                msg = JSON.parse msg
                assert_equal @time, msg["__time__"]
                assert_equal 'test',msg["__tag__"]
                assert_equal 2, msg["a"]
                redis.unsubscribe
            end
        end
    end
end
