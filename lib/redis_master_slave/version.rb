module RedisMasterSlave
  VERSION = [0, 1, 3]

  class << VERSION
    include Comparable

    def to_s
      join('.')
    end
  end
end
