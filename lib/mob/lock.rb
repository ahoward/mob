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
          include Mongoid::Document
          include Mongoid::Timestamps

          field(:hostname, :default => proc{ ::Lock.hostname })
          field(:ppid, :default => proc{ ::Lock.ppid })
          field(:pid, :default => proc{ ::Lock.pid })

          attr_accessor :stolen

          def stolen?
            stolen
          end

          def initialize(*args, &block)
            super
          ensure
            now = Time.now
            self.created_at ||= now
            self.updated_at ||= now
            @locked = false
          end

          def localhost?
            ::Lock.hostname == hostname
          end

          def alive?
            return true unless localhost?
            ::Lock.alive?(ppid, pid)
          end

          def stale?
            localhost? and not alive?
          end

          def owner?
            ::Lock.identifier == identifier
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
        def target_class.lock!(query = {}, update = {}, options = {})
          query.to_options!
          update.to_options!
          options.to_options!

          query[:_lock] = nil

          update[:$set] = {:_lock => lock_class.new.attributes}

          options[:safe] = true
          options[:new] = true

          begin
            collection.find_and_modify(
              :query => query,
              :update => update,
              :options => options
            )
          rescue Object => e
            nil
          end
        end

        def lock!(conditions = {})
          stolen = false
          begin
            if _lock and _lock.stale?
              _lock.destroy
              stolen = true
            end
          rescue
            nil
          end

          conditions.to_options!
          conditions[:_id] = id

          if self.class.lock!(conditions)
            reload

            begin
              _lock and _lock.owner?
              _lock.stolen = stolen
              @locked = _lock
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
          raise(::Lock::Error, "#{ name } is not locked!") unless @locked

          relocked = false
          if _lock
            begin
              _lock.update_attributes!(
                :updated_at => Time.now.utc,
                :hostname => ::Lock.hostname,
                :ppid => ::Lock.ppid,
                :pid => ::Lock.pid
              )
              relocked = true
            rescue
              nil
            end
            reload
          end
          relocked
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
                  raise(::Lock::Error, name)
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
    index(:name, :unique => true)

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
