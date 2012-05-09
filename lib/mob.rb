# encoding: utf-8
module Mob
  require 'fileutils'
  require 'ostruct'
  require 'rbconfig'
  require 'pathname'
  require 'socket'
  require 'pp'

  load 'mob/utils.rb'
  load 'mob/lock.rb'
  load 'mob/logger.rb'
  load 'mob/job.rb'
  load 'mob/worker.rb'
  load 'mob/script.rb'

  Mob.before_fork do
    ::Mongoid.identity_map_enabled = false if defined?(::Mongoid.identity_map_enabled)
    ::Mongoid.master.connection.close
  end

  Mob.after_fork do
    ::Mongoid.identity_map_enabled = false if defined?(::Mongoid.identity_map_enabled)
    ::Mongoid.master.connection.connect

    if defined?(CentralLogger)
      begin
        initializer = ::Rails.application.initializers.detect{|_| _.name =~ /initialize_central_logger/}
        initializer.run if initializer
      rescue Object => e
      end
    end
  end
end
