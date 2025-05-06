<?php

declare(strict_types=1);

namespace Thesis\MessageBus\Transport\Amqp;

use Thesis\MessageBus\Stamp;
use Thesis\Time\TimeSpan;

/**
 * @api
 */
final readonly class AmqpExpiration implements Stamp
{
    public function __construct(
        public TimeSpan $expiration,
    ) {}
}
