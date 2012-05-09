module Mob
  class Logger
    attr_accessor :logger
    attr_accessor :prefix

    def initialize(logger, options = {})
      @logger = logger
      @prefix = options[:prefix] || :Mob
    end

    def method_missing(method, *args, &block)
      super unless @logger.respond_to?(method)
      @logger.send(method, *args, &block)
    end

    Levels = %w( debug info warn error fatal )

    LevelValues = Hash[ Levels.each_with_index.to_a ]

    def level_value_for(level)
      LevelValues[level.to_s] || LevelValues['fatal']
    end

    Levels.each do |level|
      module_eval <<-__
        def #{ level }(*args, &block)
          begin
            return nil if @logger.level > level_for(#{ level.inspect })
          rescue Object => e
          end

          a = nil
          b = nil

          if prefix
            message = (block ? block.call : args.join(' ')).to_s
            prefixed_message = [prefix, message].join(' - ')
            a = [prefixed_message]
            b = nil
          else
            a = args
            b = block
          end

          begin
            @logger.#{ level }(*a, &b)
          rescue Object => e
            failsafe = ::Logger.new(STDERR)
            failsafe.error(e) rescue nil
            failsafe.#{ level }(*a, &b) rescue nil
            @logger = failsafe
          end
        end
      __
    end
  end
end
