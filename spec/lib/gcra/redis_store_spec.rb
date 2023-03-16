# frozen_string_literal: true

require 'spec_helper'
require 'digest/sha1'
require_relative '../../../lib/gcra/rate_limiter'
require_relative '../../../lib/gcra/redis_store'

RSpec.describe GCRA::RedisStore do
  # Needs redis running on localhost:6379 (default port)
  let(:redis)      { RedisClient.new }
  let(:key_prefix) { 'gcra-ruby-specs:' }
  let(:options)    { {} }
  let(:store)      { described_class.new(redis, key_prefix, options) }

  def cleanup_redis
    keys = redis.call('KEYS', "#{key_prefix}*")
    unless keys.empty?
      redis.call('DEL', keys)
    end
  end

  before do
    allow(redis).to receive(:call).and_call_original

    begin
      redis.call('PING')
    rescue RedisClient::CannotConnectError
      pending('Redis is not running on localhost:6379, skipping')
    end

    cleanup_redis
  end

  after do
    cleanup_redis
  end

  specify "canary: CAS_SHA is up to date" do
    actual_sha = Digest::SHA1.hexdigest(GCRA::RedisStore::CAS_SCRIPT)
    stored_sha = GCRA::RedisStore::CAS_SHA
    expect(actual_sha).to eq(stored_sha),
      "CAS_SCRIPT was updated without adjusting CAS_SHA! Please change CAS_SHA to '#{stored_sha}'"
  end

  describe '#get_with_time' do
    it 'with a value set, returns time and value' do
      redis.call('SET', 'gcra-ruby-specs:foo', 1_485_422_362_766_819_000)

      value, time = store.get_with_time('foo')

      expect(value).to eq(1_485_422_362_766_819_000)
      expect(time).to be > 1_000_000_000_000_000_000
      expect(time).to be < 3_000_000_000_000_000_000
    end

    it 'with no value set, returns time and value' do
      value, time = store.get_with_time('foo')

      expect(value).to eq(nil)
      expect(time).to be > 1_000_000_000_000_000_000
      expect(time).to be < 3_000_000_000_000_000_000
    end
  end

  describe '#set_if_not_exists_with_ttl' do
    it 'with an existing key, returns false' do
      redis.call('SET', 'gcra-ruby-specs:foo', 1_485_422_362_766_819_000)

      did_set = store.set_if_not_exists_with_ttl('foo', 2_000_000_000_000_000_000, 1)

      expect(did_set).to eq(false)
    end

    it 'with a readonly host and no readonly configured' do
      exception = RedisClient::CommandError.new(GCRA::RedisStore::CONNECTED_TO_READONLY)

      expect(redis)
        .to receive(:call)
              .with('SET', 'gcra-ruby-specs:foo', 2_000_000_000_000_000_000, nx: true, px: 1)
              .and_raise(exception)

      expect do
        store.set_if_not_exists_with_ttl('foo', 2_000_000_000_000_000_000, 1)
      end.to raise_error(exception)
    end

    context 'with reconnect on readonly configured' do
      let(:options) do
        {
          reconnect_on_readonly: true,
        }
      end

      it 'with a readonly host' do
        exception = RedisClient::CommandError.new(GCRA::RedisStore::CONNECTED_TO_READONLY)

        expect(redis.send(:raw_connection))
          .to receive(:reconnect)
                .and_call_original

        expect(redis)
          .to receive(:call)
                .with('SET', 'gcra-ruby-specs:foo', 2_000_000_000_000_000_000, nx: true, px: 1)
                .and_raise(exception)

        expect(redis)
          .to receive(:call)
                .with('SET', 'gcra-ruby-specs:foo', 2_000_000_000_000_000_000, nx: true, px: 1)
                .and_call_original

        store.set_if_not_exists_with_ttl('foo', 2_000_000_000_000_000_000, 1)
      end
    end

    it 'with no existing key, returns true' do
      did_set = store.set_if_not_exists_with_ttl(
        'foo', 3_000_000_000_000_000_000, 10 * 1_000_000_000
      )

      expect(did_set).to eq(true)
      expect(redis.call('TTL', 'gcra-ruby-specs:foo')).to be > 8
      expect(redis.call('TTL', 'gcra-ruby-specs:foo')).to be <= 10
    end

    it 'with a very low ttl (less than 1ms)' do
      did_set = store.set_if_not_exists_with_ttl(
        'foo', 3_000_000_000_000_000_000, 100
      )

      expect(did_set).to eq(true)
      expect(redis.call('TTL', 'gcra-ruby-specs:foo')).to be <= 1
    end
  end

  describe '#compare_and_set_with_ttl' do
    it 'with no existing key, returns false' do
      swapped = store.compare_and_set_with_ttl(
        'foo', 2_000_000_000_000_000_000, 3_000_000_000_000_000_000, 1 * 1_000_000_000
      )

      expect(swapped).to eq(false)
      expect(redis.call('GET', 'gcra-ruby-specs:foo')).to be_nil
    end

    it 'with an existing key and not matching old value, returns false' do
      redis.call('SET', 'gcra-ruby-specs:foo', 1_485_422_362_766_819_000)

      swapped = store.compare_and_set_with_ttl(
        'foo', 2_000_000_000_000_000_000, 3_000_000_000_000_000_000, 10 * 1_000_000_000
      )

      expect(swapped).to eq(false)
      expect(redis.call('GET', 'gcra-ruby-specs:foo')).to eq('1485422362766819000')
    end

    it 'with an existing key and matching old value, returns true' do
      redis.call('SET', 'gcra-ruby-specs:foo', 2_000_000_000_000_000_000)

      swapped = store.compare_and_set_with_ttl(
        'foo', 2_000_000_000_000_000_000, 3_000_000_000_000_000_000, 10 * 1_000_000_000
      )

      expect(swapped).to eq(true)
      expect(redis.call('GET', 'gcra-ruby-specs:foo')).to eq('3000000000000000000')
      expect(redis.call('TTL', 'gcra-ruby-specs:foo')).to be > 8
      expect(redis.call('TTL', 'gcra-ruby-specs:foo')).to be <= 10
    end

    it 'with an existing key and a very low ttl (less than 1ms)' do
      redis.call('SET', 'gcra-ruby-specs:foo', 2_000_000_000_000_000_000)

      swapped = store.compare_and_set_with_ttl(
        'foo', 2_000_000_000_000_000_000, 3_000_000_000_000_000_000, 100
      )

      expect(swapped).to eq(true)
      expect(redis.call('TTL', 'gcra-ruby-specs:foo')).to be <= 1
    end

    it 'handles the script cache being purged (gracefully reloads script)' do
      redis.call('SET', 'gcra-ruby-specs:foo', 2_000_000_000_000_000_000)

      swapped = store.compare_and_set_with_ttl(
        'foo', 2_000_000_000_000_000_000, 3_000_000_000_000_000_000, 10 * 1_000_000_000
      )

      expect(swapped).to eq(true)
      expect(redis.call('GET', 'gcra-ruby-specs:foo')).to eq('3000000000000000000')
      expect(redis.call('TTL', 'gcra-ruby-specs:foo')).to be > 8
      expect(redis.call('TTL', 'gcra-ruby-specs:foo')).to be <= 10

      # purge the script cache, this will trigger an exception branch that reloads the script
      redis.call('SCRIPT', 'FLUSH')

      swapped = store.compare_and_set_with_ttl(
        'foo', 3_000_000_000_000_000_000, 4_000_000_000_000_000_000, 10 * 1_000_000_000
      )

      expect(swapped).to eq(true)
      expect(redis.call('GET', 'gcra-ruby-specs:foo')).to eq('4000000000000000000')
      expect(redis.call('TTL', 'gcra-ruby-specs:foo')).to be > 8
      expect(redis.call('TTL', 'gcra-ruby-specs:foo')).to be <= 10
    end

    context 'with reconnect on readonly not configured' do
      it 'raises an error when the request is executed against a readonly host' do
        exception = RedisClient::CommandError.new(GCRA::RedisStore::CONNECTED_TO_READONLY)

        expect(redis)
          .to receive(:call)
                .with(
                  'EVALSHA',
                  GCRA::RedisStore::CAS_SHA,
                  1,
                  'gcra-ruby-specs:foo',
                  3000000000000000000,
                  4000000000000000000,
                  10000,
                )
                .and_raise(exception)

        expect do
          store.compare_and_set_with_ttl(
            'foo',
            3_000_000_000_000_000_000,
            4_000_000_000_000_000_000,
            10 * 1_000_000_000,
          )
        end.to raise_error(exception)
      end
    end

    context 'with reconnect on readonly configured' do
      let(:options) do
        {
          reconnect_on_readonly: true,
        }
      end

      it 'attempts a reconnect once and then executes evalsha again' do
        exception = RedisClient::CommandError.new(GCRA::RedisStore::CONNECTED_TO_READONLY)

        expect(redis.send(:raw_connection))
          .to receive(:reconnect)
                .and_call_original

        expect(redis)
          .to receive(:call)
                .with(
                  'EVALSHA',
                  GCRA::RedisStore::CAS_SHA,
                  1,
                  'gcra-ruby-specs:foo',
                  3000000000000000000,
                  4000000000000000000,
                  10000,
                )
                .and_raise(exception)

        expect(redis)
          .to receive(:call)
                .with(
                  'EVALSHA',
                  GCRA::RedisStore::CAS_SHA,
                  1,
                  'gcra-ruby-specs:foo',
                  3000000000000000000,
                  4000000000000000000,
                  10000,
                )

        store.compare_and_set_with_ttl(
          'foo',
          3_000_000_000_000_000_000,
          4_000_000_000_000_000_000,
          10 * 1_000_000_000,
        )
      end
    end

  end

  context 'functional test with RateLimiter' do
    let(:limiter) { GCRA::RateLimiter.new(store, 1, 2) }

    it 'allow and limits properly' do
      # Attempt too high quantity
      limit1, info1 = limiter.limit('foo', 4)

      aggregate_failures do
        expect(limit1).to be true
        expect(info1.limit).to eq(3)
        expect(info1.remaining).to eq(3)
        expect(info1.reset_after).to eq(0.0)
        expect(info1.retry_after).to be_nil
      end

      # Normal request
      limit1, info1 = limiter.limit('foo', 1)

      aggregate_failures do
        expect(limit1).to be false
        expect(info1.limit).to eq(3)
        expect(info1.remaining).to eq(2)
        expect(info1.reset_after).to eq(1.0)
        expect(info1.retry_after).to be_nil
      end

      # Normal request, fills up rest of bucket
      limit1, info1 = limiter.limit('foo', 2)

      aggregate_failures do
        expect(limit1).to be false
        expect(info1.limit).to eq(3)
        expect(info1.remaining).to eq(0)
        expect(info1.reset_after).to be < 3.0
        expect(info1.reset_after).to be > 2.5
        expect(info1.retry_after).to be_nil
      end

      # Normal request, exceeds limit
      limit1, info1 = limiter.limit('foo', 1)

      aggregate_failures do
        expect(limit1).to be true
        expect(info1.limit).to eq(3)
        expect(info1.remaining).to eq(0)
        expect(info1.reset_after).to be < 3.0
        expect(info1.reset_after).to be > 2.0
        expect(info1.retry_after).to be < 1.0
        expect(info1.retry_after).to be > 0.5
      end

      # Allows a normal request after 1 second waiting
      sleep(1)
      limit1, info1 = limiter.limit('foo', 1)

      aggregate_failures do
        expect(limit1).to be false
        expect(info1.limit).to eq(3)
        expect(info1.remaining).to eq(0)
        expect(info1.reset_after).to be < 3.0
        expect(info1.reset_after).to be > 2.0
        expect(info1.retry_after).to be_nil
      end
    end
  end
end
