# ensure background job processing is running when under apache, nginx, etc
#

if ENV['SERVER_SOFTWARE']

  Thread.new do
    Thread.current.abort_on_exception = true

    script = File.join(Rails.root, 'script', 'mob')

    system "#{ script } start >/dev/null 2>&1"

    unless Rails.env.production?
      at_exit do
        system "#{ script } stop >/dev/null 2>&1"
      end
    end
  end

end
