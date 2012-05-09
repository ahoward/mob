# encoding: utf-8
module Mob
  class Script
  ##
  #
    attr_reader(:name)

    attr_accessor(:file)
    attr_accessor(:script)
    attr_accessor(:worker)
    attr_accessor(:block)
    attr_accessor(:cmdline)
    attr_accessor(:signals)

    def initialize(file, options = {}, &block)
      @file = file.to_s
      @name = File.basename(@file)
      @block = block
      @cmd = cmdline_for(ARGV)
      @started_at = Time.now
      @signals = []
      @sleeping = false
    end

    def worker
      @worker ||= Worker.for(@name)
    end

    def name=(name)
      @name = name.to_s
      @worker = Worker.for(@name)
      @name
    end

    def run(argv = ARGV)
      Dir.chdir(Rails.root)

      STDOUT.sync = true
      STDERR.sync = true

      mode = (argv.shift || :run).to_s.downcase

      abort("bad mode #{ mode.inspect }") unless respond_to?("mode_#{ mode }")

      send("mode_#{ mode }", *argv)
    end

  ##
  #
    def mode_run(*argv)
      lock!(:verbose => true)

      trap!

      log!

      run!
    end

    def mode_start
      lock!(:complain => true)

      daemonize!{|pid| p pid}

      relock!

      redirect_io!

      trap!

      signal_if_redeployed!

      log!

      run!
    end

    def mode_restart
      exit(0) if fork
      exit(0) if fork
      system "#{ cmdline_for(:stop) } >/dev/null 2>&1"
      system "#{ cmdline_for(:start) } >/dev/null 2>&1"
    end

    def mode_ping
      mode_signal(:ALRM)
    end

    def mode_stop
      mode_signal(:TERM)
    end

    def mode_signal(*argv)
      if worker_localhost?
        local_signal!(*argv)
      else
        remote_signal!(*argv)
      end
    end

    def mode_pid(*argv)
      pid = worker_pid
      if worker_localhost?
        begin
          Process.kill(0, pid)
          puts(pid) if pid
        rescue Object
        end
      else
        puts(pid)
      end
    end

    def mode_identifier
      identifier = worker_identifier
      puts(identifier) if identifier
    end

    def mode_worker
      puts(Mob.json_dump(worker.attributes, :pretty => true))
    end

  ##
  #
    def run!
      at_exit{ unlock! }

      worker.clear_inbox!
      worker.clear_signals!

      loop do
        catch(:signals) do
          process_signals

          begin
            @block.call(self) if @block
          rescue => e
            logger.error(e)
          ensure
            min, max = Rails.env.production? ? [3, 13] : [1, 3]
            timeout = [min, rand(max)].max
            pause(timeout)
          end
        end
      end
    end

    def log!
        logger.debug("START - #{ worker_identifier }")
        logger.debug("CMD   - #{ @cmd }")

      at_exit do
        logger.debug("STOP - #{ Process.pid }") rescue nil
      end
    end

    def daemonize!(options = {}, &block)
      chdir = options[:chdir] || options['chdir'] || '.'
      umask = options[:umask] || options['umask'] || 0

      Mob.before_fork()

      a, b = IO.pipe

      if fork
        b.close
        pid = Integer(a.read.strip)
        a.close
        block.call(pid) if block
        exit!
      end

      exit!(0) if fork

      a.close
      b.puts(Process.pid)
      b.close

      Process::setsid rescue nil

      keep_ios(STDIN, STDOUT, STDERR)

      Dir::chdir(chdir)
      File::umask(umask)

      $DAEMON = true

      at_exit{ exit! }

      Mob.after_fork()
    end

    def redirect_io!
      stdin, stdout, stderr =
        %w( stdin stdout stderr ).map do |basename|
          path = File.join(Rails.root, 'tmp', 'mob', @name, basename)
          FileUtils.mkdir_p(File.dirname(path))
          path
        end

      {
        STDIN => stdin, STDOUT => stdout, STDERR => stderr
      }.each do |io, file|
        open(file, 'a+') do |fd|
          fd.sync = true
          fd.truncate(0)
          io.reopen(fd)
        end
      end
    end

    def keep_ios(*ios)
      filenos = []

      ios.flatten.compact.each do |io|
        begin
          fileno = io.respond_to?(:fileno) ? io.fileno : Integer(io)
          filenos.push(fileno)
        rescue Object
          next
        end
      end

      ObjectSpace.each_object(IO) do |io|
        begin
          fileno = io.fileno
          next if filenos.include?(fileno)
          io.close unless io.closed?
        rescue Object
          next
        end
      end
    end

    def lock!(options = {})
      options.to_options!

      lock = worker.lock!

      unless lock
        logger.warn("instance(#{ worker.identifier }) is already running!") if options[:verbose]
        exit(42)
      end

      if lock.stolen?
        logger.warn("stolen lock!") if options[:verbose]
      end

      at_exit{ unlock! }

      lock
    end

    def unlock!
      worker.unlock!
    end

    def relock!
      worker.relock!
    end

    def worker_pid
      _lock = worker.reload._lock
      _lock.pid if _lock
    rescue
      nil
    end

    def worker_identifier
      worker.reload

      Mob.json_dump(
        'name'     => worker.name,
        'hostname' => worker.hostname,
        'ppid'     => worker.ppid,
        'pid'      => worker.pid
      )
    rescue
      nil
    end

    def worker_hostname
      _lock = worker.reload._lock
      _lock.hostname if _lock
    rescue
      nil
    end

    def worker_localhost?
      hostname == worker_hostname
    rescue
      false
    end

    def hostname
      Socket.gethostname
    end

    def pause(seconds)
      begin
        @sleeping = true
        ::Kernel.sleep(seconds)
      ensure
        @sleeping = false
      end
    end

    def sleeping?(&block)
      if block
        block.call if @sleeping
      else
        @sleeping == true
      end
    end

    def trap!
      %w( SIGHUP SIGALRM SIGUSR1 SIGUSR2 SIGINT SIGTERM ).each do |signal|
        trap(signal){|sig| signal!(signal)}
      end
    end

    def signal!(signal)
      signals.push(signal)
      signals.uniq!
      throw(:signals, signal) if sleeping?
    end

    def clear_signals!
      signals.clear
    end

    def process_signals
      worker.reload
      all_signals = (signals + worker.signals).uniq

      clear_signals!
      worker.clear_signals!

      all_signals.each do |signal|
        logger.debug("SIGNAL - #{ signal }")

        case signal.to_s
          when /INT/i
            exit
          when /TERM/i
            exit
          when /HUP/i
            restart!
          when /USR1/i
            restart!
          when /USR2/i
            nil
          when /ALRM/i
            nil
        end
      end
    end

    def local_signal!(*argv)
      pid = worker_pid
      if pid
        begin
          signal = (argv.shift || :ALRM).to_s
          Process.kill(signal, pid)
          puts(pid)
        rescue
          nil
        end
      end
    rescue
      nil
    end

    def remote_signal!(*argv)
      signal = (argv.shift || :ALRM).to_s
      worker.signal!(signal)
    rescue
      nil
    end

    def signal_if_redeployed!
      seconds = Rails.env.production? ? 10 : 1

      Thread.new do
        Thread.current.abort_on_exception = true

        loop do
          Kernel.sleep(seconds)

          if redeployed?
            logger.debug("REDEPLOYED - #{ Process.pid }")
            Process.kill('USR1', Process.pid)
          end
        end
      end
    end

    def redeployed?
      restart_txt = current_path_for(File.join(Rails.root, 'tmp/restart.txt'))
      t = File.stat(restart_txt).mtime rescue @started_at
      if t > @started_at
        @started_at = t
        true
      else
        false
      end
    end

    def cmdline_for(argv)
      script = current_path_for(file)
      [Mob.which_ruby, script, *argv].join(' ')
    end

    def current_path_for(path)
      path.to_s.gsub(%r|\breleases/\d+\b|, 'current')
    end

    def loggers
      @loggers ||= Hash.new do |hash, pid|
        tty_loggers = {
          true  => Mob::Logger.new(::Logger.new(STDERR), :prefix => "Mob[#{ @name }]"),
          false => Mob::Logger.new(Rails.logger, :prefix => "Mob[#{ @name }]")
        }
        hash.update(pid => tty_loggers)
      end
    end

    def logger
      loggers[Process.pid]
      loggers[Process.pid][STDOUT.tty?]
    end
  end
end
