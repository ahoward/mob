module Mob
##
#
  def Mob.job(*args, &block)
    Job.submit(*args, &block)
  end

  def Mob.submit(*args, &block)
    Job.submit(*args, &block)
  end

  def Mob.jobs(*args, &block)
    Job.run(*args, &block)
  end

  class Job
  ##
  #
    include ::Mongoid::Document
    include ::Mongoid::Timestamps

    class Literal
      include Mongoid::Fields::Serializable

      def serialize(object) object.to_s unless object.nil? end
      def deserialize(string) eval(string) unless string.nil? end
    end

    class Serialized
      include Mongoid::Fields::Serializable

      def serialize(object) Mob.json_dump(object) end
      def deserialize(string) Mob.json_load(string) end
    end

  ##
  #
    field(:job, :type => Literal)
    field(:args, :type => Serialized)
    field(:result, :type => Serialized, :default => nil)
    field(:error, :type => Hash, :default => nil)

    field(:run_at, :type => Time, :default => proc{ Time.now.utc })
    field(:completed_at, :type => Time, :default => nil)
    field(:attempts, :type => Integer, :default => 0)

    field(:locked_by, :type => String, :default => nil)
    field(:locked_at, :type => String, :default => nil)

  ##
  #
    validates_presence_of(:job)
    validates_presence_of(:args)
    validates_presence_of(:run_at)
    validates_presence_of(:attempts)

  ##
  #
    index(:job)
    index(:created_at)
    index(:locked_by)
    index(:locked_at)
    index(:completed_at)
    index(:attempts)

  ##
  #
    def Job.submit(*args, &block)
      make!(*args, &block)
    end

    def Job.submit_all(argv, &block)
      argv.map{|args| make!(*args, &block)}
    end

    def Job.make!(*args, &block)
      args.compact!
      attributes = {}
      attributes[:job] = "::#{ args.shift }" unless args.empty?
      attributes[:args] = args
      create!(attributes)
    end

    def Job.purge!(max = 8192)
      current_count = Job.count

      if current_count > max
        where(:completed_at.lt => 1.minute.ago).
          delete_all
      end

      if current_count > max
        where(:attempts.gt => 4).
          delete_all
      end
      
      where(:completed_at.lt => 1.week.ago).
        delete_all
    end

    def Job.run(options = {}, &block)
      Job.purge!

      locked_by = (
        options[:locked_by] || options['locked_by'] || 
        options[:as] || options['as'] ||
        'worker'
      ).to_s

      now = Time.now.utc

      query = {
        'completed_at' => nil,
        'run_at'       => {'$lt' => now},
        'updated_at'   => {'$lt' => now},
        'attempts'     => {'$lt' => 16},

        '$or' => [
          { 'locked_by' => locked_by },
          { 'locked_by' => nil }
        ]
      }

      sort = [
        ['attempts', 1]
      ]

      n = 0

      loop do
        now = Time.now.utc

        update = {
          '$set' => {
            :locked_by  => locked_by,
            :locked_at  => now,
            :updated_at => now
          }
        }

        result =
          collection.find_and_modify(
            :query  => query,
            :sort   => sort,
            :update => update
          )

        break unless result

        job = instantiate(result)
        n += 1
        job.run

        block.call(job) if block
      end

      n
    end

    def run
      self.class.collection.find_and_modify(
        :query => {
          :_id => id
        },
        :update => {
          '$set' => {
            :attempts => (attempts + 1)
          }
        }
      )

      begin
        result = job.perform(*args)
        now = Time.now.utc
        result = Mob.json_dump(result)

        self.class.collection.find_and_modify(
          :query => {
            :_id => id
          },
          :update => {
            '$set' => {
              :result       => result,
              :completed_at => now,
              :updated_at   => now,
              :locked_by    => nil,
              :locked_at    => nil
            }
          }
        )
      rescue Object => e
        now = Time.now.utc

        error = {
          'message'   => e.message.to_s,
          'class'     => e.class.name,
          'backtrace' => (e.backtrace || [])
        }

        later = now + 60

        self.class.collection.find_and_modify(
          :query => {
            :_id => id
          },
          :update => {
            '$set' => {
              :error      => error,
              :run_at     => later,
              :updated_at => now,
              :locked_by  => nil,
              :locked_at  => nil
            }
          }
        )
      end

      reload
    end

    def inspect
      PP.pp(self.attributes, '')
    end
  end
end
