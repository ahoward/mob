# encoding: utf-8
module Mob
  class Worker
    include Mongoid::Document
    include Mongoid::Timestamps
    include Mob::Lock.ability

    def Worker.for(name, options = {})
      name = name.to_s
      conditions = {:name => name}

      attributes = conditions.dup
      attributes[:created_at] || attributes[:updated_at] = Time.now.utc

      worker =
        begin
          where(conditions).first or create!(attributes)
        rescue Object => e
          sleep(rand)
          where(conditions).first or create!(attributes)
        end
    end

    class Message
      include Mongoid::Document
      include Mongoid::Timestamps

      field(:kind, :type => String)
      field(:content, :type => String)

      embedded_in(:worker, :class_name => Worker.name)
    end
    embeds_many(:inbox, :class_name => Message.name)
    embeds_many(:outbox, :class_name => Message.name)

    field(:name, :type => String)
    field(:signals, :type => Array, :default => [])

    validates_presence_of(:name)
    validates_uniqueness_of(:name)

    index({:name => 1}, {:unique => true})

    def work(&block)
      Mob::Job.run(:locked_by => name) do |job|
        block.call(job) if block
      end
    end

    def signal!(signal)
      add_to_set(:signals, signal.to_s)
    end

    def clear_signals!
      update_attributes!(:signals => [])
    end

    def clear_inbox!
      inbox.destroy_all
    end

    def identifier
      _lock.try(:identifier)
    end

    def hostname
      _lock.try(:hostname)
    end

    def ppid
      _lock.try(:ppid)
    end

    def pid
      _lock.try(:pid)
    end
  end
end
