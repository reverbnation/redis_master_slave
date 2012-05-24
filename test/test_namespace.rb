require 'test_helper'

class Rails
  class Log
    def self.debug(str)
      puts str
    end
  end

  def self.logger
    Log
  end
end

class NamespaceTest < Test::Unit::TestCase
  def test_namespace_nil
    rms = RedisMasterSlave.new(OPTIONS_HASH)
    r=Redis.new(OPTIONS_HASH)

    assert_nil(rms.namespace)
    assert_nil(rms.namespace_rms)
    rms.set("a","2")
    assert_equal("2", rms.get("a"))
    assert_equal("2", r.get("a"))
    rms.set("a","3")
    assert_equal("3", rms.get("a"))
    assert_equal("3", r.get("a"))
  end

  def test_namespace_non_nil
    rms = RedisMasterSlave.new(OPTIONS_HASH.merge({:namespace => "nachos"}))
    r=Redis.new(OPTIONS_HASH)

    assert_equal("nachos", rms.namespace)
    assert_equal("nachos", rms.namespace_rms)
    rms.set("a","2")
    assert_equal("2", rms.get("a"))
    assert_equal("2", r.get("nachos:a"))
    rms.set("a","3")
    assert_equal("3", rms.get("a"))
    assert_equal("3", r.get("nachos:a"))
  end

  def test_namespace_set_runtime
    rms = RedisMasterSlave.new(OPTIONS_HASH)

    assert_nil(rms.namespace)
    assert_nil(rms.namespace_rms)
    rms.namespace = "popcorn"
    assert_equal("popcorn", rms.namespace)
    assert_equal("popcorn", rms.namespace_rms)

    r=Redis.new(OPTIONS_HASH)
    rms.set("a","2")
    assert_equal("2", rms.get("a"))
    assert_equal("2", r.get("popcorn:a"))
    rms.set("a","3")
    assert_equal("3", rms.get("a"))
    assert_equal("3", r.get("popcorn:a"))
  end

  def test_namespace_set_runtime_with_string
    rms = RedisMasterSlave.new(OPTIONS_STR)

    assert_nil(rms.namespace)
    assert_nil(rms.namespace_rms)
    rms.namespace = "pickles"
    assert_equal("pickles", rms.namespace)
    assert_equal("pickles", rms.namespace_rms)

    r=Redis.new(OPTIONS_HASH)
    rms.set("a","2")
    assert_equal("2", rms.get("a"))
    assert_equal("2", r.get("pickles:a"))
    rms.set("a","3")
    assert_equal("3", rms.get("a"))
    assert_equal("3", r.get("pickles:a"))
  end

end
