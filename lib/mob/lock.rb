# encoding: utf-8
module Mob
  class Lock
    module Ability
      Code = proc do
      ## embedded lock class and associations
      #
        target_class = self

        class << target_class
          attr_accessor :lock_class
        end

        const_set(:Lock, Class.new)
        lock_class = const_get(:Lock)

        target_class.lock_class = lock_class

        lock_class.class_eval do
          define_method(:target_class){ target_class }
          define_method(:lock_class){ lock_class }

          include Mongoid::Document
          include Mongoid::Timestamps

          field(:hostname, :default => proc{ ::Mob::Lock.hostname })
          field(:ppid, :default => proc{ ::Mob::Lock.ppid })
          field(:pid, :default => proc{ ::Mob::Lock.pid })

          attr_accessor :stolen
          alias_method :stolen?, :stolen

          def initialize(*args, &block)
            super
          ensure
            now = Time.now
            self.created_at ||= now
            self.updated_at ||= now
            @locked = false
          end

          def localhost?
            ::Mob::Lock.hostname == hostname
          end

          def alive?
            return true unless localhost?
            ::Mob::Lock.alive?(ppid, pid)
          end

          def relock!
            reload

            conditions = {
              '_lock._id'      => id,
              '_lock.hostname' => hostname,
              '_lock.ppid'     => ppid,
              '_lock.pid'      => pid
            }

            update = {
              '$set' => {
                '_lock.hostname'   => ::Mob::Lock.hostname,
                '_lock.ppid'       => ::Mob::Lock.ppid,
                '_lock.pid'        => ::Mob::Lock.pid,
                '_lock.updated_at' => Time.now.utc
              }
            }

            result =
                target_class.
                  with(safe: true).
                    where(conditions).
                      find_and_modify(update, new: false)

          ensure
            reload
          end

          def steal!
            self.stolen = !!relock!
          end

          def stale?
            localhost? and not alive?
          end

          def owner?
            ::Mob::Lock.identifier == identifier
          end

          def identifier
            {:hostname => hostname, :ppid => ppid, :pid => pid}
          end
        end

        target_association_name = "_" + target_class.name.underscore.split(%r{/}).last
        
        lock_class.class_eval do
          embedded_in(target_association_name, :class_name => "::#{ target_class.name }")
        end

        embeds_one(:_lock, :class_name => "::#{ lock_class.name }")

      ## locking methods
      #
        def target_class.lock!(conditions = {}, update = {})
          conditions.to_options!
          update.to_options!

          conditions[:_lock] = nil

          update[:$set] = {:_lock => lock_class.new.attributes}

          with(safe: true).
            where(conditions).
              find_and_modify(update, new: true)
        end

        def lock!(conditions = {})
          conditions.to_options!

          begin
            if _lock and _lock.stale? and _lock.steal!
              return _lock
            end
          rescue
            nil
          end

          conditions[:_id] = id

          if self.class.lock!(conditions)
            reload

            begin
              @locked = _lock && _lock.owner?
            rescue
              nil
            end
          else
            false
          end
        end

        def unlock!
          unlocked = false

          if _lock
            begin
              _lock.destroy if _lock.owner?
              @locked = false
              unlocked = true
            rescue
              nil
            end

            reload
          end

          unlocked
        end

        def relock!
          raise(::Mob::Lock::Error, "#{ name } is not locked!") unless @locked

          _lock.relock!
        end

        def locked?
          begin
            _lock and _lock.owner?
          rescue
            nil
          end
        end

        def lock(options = {}, &block)
          options.to_options!

          return block.call(_lock) if locked?

          loop do
            if lock!
              return _lock unless block

              begin
                return block.call(_lock)
              ensure
                unlock!
              end
            else
              if options[:blocking] == false
                if block
                  raise(::Mob::Lock::Error, name)
                else
                  return(false)
                end
              end

              if options[:waiting]
                options[:waiting].call(reload._lock)
              end

              sleep(rand)
            end
          end
        end
      end

      def Ability.included(other)
        super
      ensure
        other.module_eval(&Code)
      end
    end

    def Lock.ability
      Ability
    end

  ##
  #
    include Mongoid::Document
    include Mongoid::Timestamps

  ##
  #
    class Error < ::StandardError; end

  ##
  #
    def Lock.for(name, options = {}, &block)
      name = name.to_s
      conditions = {:name => name}

      attributes = conditions.dup
      attributes[:created_at] || attributes[:updated_at] = Time.now.utc

      lock =
        begin
          where(conditions).first or create!(attributes)
        rescue Object => e
          sleep(rand)
          where(conditions).first or create!(attributes)
        end

      block ? lock.lock(options, &block) : lock
    end

  ##
  #
    field(:name)

    validates_presence_of(:name)
    validates_uniqueness_of(:name)

    index({:name => 1}, {:unique => true})

  ##
  #
    def Lock.hostname
      Socket.gethostname
    end

    def Lock.ppid
      Process.ppid
    end

    def Lock.pid
      Process.pid
    end

    def Lock.identifier
      {:hostname => hostname, :ppid => ppid, :pid => pid}
    end

    def Lock.alive?(*pids)
      pids.flatten.compact.all? do |pid|
        begin
          Process.kill(0, Integer(pid))
          true
        rescue Object
          false
        end
      end
    end

  ##
  #
    include Lock.ability
  end
end
