<?php

declare(strict_types=1);

namespace Thesis\MessageBus\Transport\Amqp;

use Thesis\MessageBus\Stamp;

/**
 * @api
 */
final readonly class AmqpPriority implements Stamp
{
    /**
     * @param int<0, 9> $priority
     */
    public function __construct(
        public int $priority,
    ) {}
}
