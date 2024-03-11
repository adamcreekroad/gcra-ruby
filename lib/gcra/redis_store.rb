# frozen_string_literal: true

require 'redis-client'

module GCRA
  # Redis store, expects all timestamps and durations to be integers with nanoseconds since epoch.
  class RedisStore
    CAS_SCRIPT = <<~LUA
      local v = redis.call('get', KEYS[1])
      if v == false then
        return redis.error_reply("key does not exist")
      end
      if v ~= ARGV[1] then
        return 0
      end
      redis.call('psetex', KEYS[1], ARGV[3], ARGV[2])
      return 1
    LUA

    # Digest::SHA1.hexdigest(CAS_SCRIPT)
    CAS_SHA = '925e92682083f854e28ca3344eeb13820015453a'

    CAS_SCRIPT_MISSING_KEY_RESPONSE = 'key does not exist'
    CAS_SCRIPT_MISSING_KEY_RESPONSE_REGEX = /\A#{CAS_SCRIPT_MISSING_KEY_RESPONSE}/.freeze

    SCRIPT_NOT_IN_CACHE_RESPONSE = 'NOSCRIPT No matching script. Please use EVAL.'
    SCRIPT_NOT_IN_CACHE_RESPONSE_REGEX = /\A#{SCRIPT_NOT_IN_CACHE_RESPONSE}/.freeze

    CONNECTED_TO_READONLY = "READONLY You can't write against a read only slave."
    CONNECTED_TO_READONLY_REGEX = /\A#{CONNECTED_TO_READONLY}/.freeze

    def initialize(redis, key_prefix, options = {})
      @redis = redis
      @key_prefix = key_prefix

      @reconnect_on_readonly = options[:reconnect_on_readonly] || false
    end

    # Returns the value of the key or nil, if it isn't in the store.
    # Also returns the time from the Redis server, with microsecond precision.
    def get_with_time(key)
      time_response, value = @redis.pipelined do |pipeline|
        pipeline.call('TIME') { |t| t.map(&:to_i) } # returns tuple (seconds since epoch, microseconds)
        pipeline.call('GET', @key_prefix + key)
      end
      # Convert tuple to nanoseconds
      time = (time_response[0] * 1_000_000 + time_response[1]) * 1_000
      if value != nil
        value = value.to_i
      end

      return value, time
    end

    # Set the value of key only if it is not already set. Return whether the value was set.
    # Also set the key's expiration (ttl, in seconds).
    def set_if_not_exists_with_ttl(key, value, ttl_nano)
      full_key = @key_prefix + key
      retried = false
      begin
        ttl_milli = calculate_ttl_milli(ttl_nano)
        @redis.call('SET', full_key, value, nx: true, px: ttl_milli) == 'OK'
      rescue RedisClient::CommandError => e
        if e.message.match?(CONNECTED_TO_READONLY_REGEX) && @reconnect_on_readonly && !retried
          @redis.send(:raw_connection).reconnect
          retried = true
          retry
        end
        raise
      end
    end

    # Atomically compare the value at key to the old value. If it matches, set it to the new value
    # and return true. Otherwise, return false. If the key does not exist in the store,
    # return false with no error. If the swap succeeds, update the ttl for the key atomically.
    def compare_and_set_with_ttl(key, old_value, new_value, ttl_nano)
      full_key = @key_prefix + key
      retried = false
      begin
        ttl_milli = calculate_ttl_milli(ttl_nano)
        swapped = @redis.call('EVALSHA', CAS_SHA, 1, full_key, old_value, new_value, ttl_milli)
      rescue RedisClient::CommandError => e
        if e.message.match?(CAS_SCRIPT_MISSING_KEY_RESPONSE_REGEX)
          return false
        elsif e.message.match?(SCRIPT_NOT_IN_CACHE_RESPONSE_REGEX) && !retried
          @redis.call('SCRIPT', 'LOAD', CAS_SCRIPT)
          retried = true
          retry
        elsif e.message.match?(CONNECTED_TO_READONLY_REGEX) && @reconnect_on_readonly && !retried
          @redis.send(:raw_connection).reconnect
          retried = true
          retry
        end
        raise
      end

      return swapped == 1
    end

    private

    def calculate_ttl_milli(ttl_nano)
      ttl_milli = ttl_nano / 1_000_000
      # Setting 0 as expiration/ttl would result in an error.
      # Therefore overwrite it and use 1
      if ttl_milli == 0
        return 1
      end
      return ttl_milli
    end
  end
end
