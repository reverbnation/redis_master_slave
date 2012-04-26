module RedisMasterSlave
  VERSION = [0, 1, 4]

  class << VERSION
    include Comparable

    def to_s
      join('.')
    end
  end
end
