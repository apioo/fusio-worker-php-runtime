<?php

namespace Fusio\Worker\Runtime;

use Fusio\Worker\ResponseLog;
use Psr\Log\LoggerInterface;

class Logger implements LoggerInterface
{
    /**
     * @var ResponseLog[]
     */
    private array $logs = [];

    public function emergency(string|\Stringable $message, array $context = []): void
    {
        $this->log('EMERGENCY', $message);
    }

    public function alert(string|\Stringable $message, array $context = []): void
    {
        $this->log('ALERT', $message);
    }

    public function critical(string|\Stringable $message, array $context = []): void
    {
        $this->log('CRITICAL', $message);
    }

    public function error(string|\Stringable $message, array $context = []): void
    {
        $this->log('ERROR', $message);
    }

    public function warning(string|\Stringable $message, array $context = []): void
    {
        $this->log('WARNING', $message);
    }

    public function notice(string|\Stringable $message, array $context = []): void
    {
        $this->log('NOTICE', $message);
    }

    public function info(string|\Stringable $message, array $context = []): void
    {
        $this->log('INFO', $message);
    }

    public function debug(string|\Stringable $message, array $context = []): void
    {
        $this->log('DEBUG', $message);
    }

    public function log($level, string|\Stringable $message, array $context = []): void
    {
        $log = new ResponseLog();
        $log->setLevel($level);
        $log->setMessage((string) $message);

        $this->logs[] = $log;
    }

    public function getLogs(): array
    {
        return $this->logs;
    }
}
