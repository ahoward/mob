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

##
#
  class Job
  ##
  #
    include ::Mongoid::Document
    include ::Mongoid::Timestamps

  ##
  #
    before_save do |job|
      job.normalize!
    end

  ##
  #
    field(:receiver, :type => String, :default => 'Kernel')
    field(:message, :type => String, :default => 'eval')
    field(:args)
    field(:result)
    field(:error)

    field(:run_at, :type => Time, :default => proc{ Time.now.utc })
    field(:completed_at, :type => Time)
    field(:attempts, :type => Integer, :default => 0)

    field(:locked_by, :type => String)
    field(:locked_at, :type => String)

  ##
  #
    validates_presence_of(:receiver)
    validates_presence_of(:message)
    validates_presence_of(:args)
    validates_presence_of(:run_at)
    validates_presence_of(:attempts)

  ##
  #
    index({:receiver => 1})
    index({:message => 1})
    index({:created_at => 1})
    index({:locked_by => 1})
    index({:locked_at => 1})
    index({:run_at => 1})
    index({:completed_at => 1})
    index({:attempts => 1})

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
      attributes[:receiver] = args.shift.to_s unless args.empty?
      attributes[:message] = args.shift.to_s unless args.empty?
      attributes[:args] = args
      create!(attributes)
    end

    def Job.purge!(max = 8192)
      if Job.count > max
        where(:completed_at.lt => 1.minute.ago).
          delete_all
      end

      if Job.count > max
        where(:attempts.gt => 4).
          delete_all
      end
      
      if Job.count > max
        where(:completed_at.lt => 1.week.ago).
          delete_all
      end
    end

    def Job.run(options = {}, &block)
      Job.purge!

      locked_by = (
        options[:locked_by] || options['locked_by'] ||
        options[:as]        || options['as']        ||
        'mob'
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
            'locked_by'  => locked_by,
            'locked_at'  => now,
            'updated_at' => now
          }
        }

        jobs = where(query)

        break unless jobs.count > 0

        job =
          with(:safe => true).
            where(query).
              order_by(sort).
                find_and_modify(update)

        break unless job

        n += 1

        job.run

        block.call(job) if block
      end

      n
    end

    def run
      inc(:attempts, 1)

      begin
        job = eval(receiver, TOPLEVEL_BINDING)

        result = job.send(message, *args)

        now    = Time.now.utc
        result = Mob.pod(result)

        update_attributes!(
          'result'       => result,
          'completed_at' => now,
          'updated_at'   => now,
          'locked_by'    => nil,
          'locked_at'    => nil
        )
      rescue Object => e
        now = Time.now.utc

        error = {
          'message'   => e.message.to_s,
          'class'     => e.class.name.to_s,
          'backtrace' => (e.backtrace || [])
        }

        later = now + ((2 ** [attempts - 1, 10].min) * 60)

        update_attributes!(
          'error'      => error,
          'run_at'     => later,
          'updated_at' => now,
          'locked_by'  => nil,
          'locked_at'  => nil
        )
      end

      reload
    end

    def normalize!
      tap do |job|
        job.receiver = job.receiver.to_s unless job.receiver.nil?
        job.message = job.message.to_s unless job.message.nil?

        job.args = Mob.pod(job.args) unless job.args.nil?
        job.result = Mob.pod(job.result) unless job.result.nil?
        job.error = Mob.pod(job.error) unless job.error.nil?
      end
    end

    def Job.field_names
      @field_names ||= fields.map{|name, *_| name}
    end

    def to_hash
      hash = {}
      Job.field_names.each{|field| hash[field] = self[field]}
      hash
    end

    def inspect
      PP.pp(to_hash, '')
    end
  end
end
