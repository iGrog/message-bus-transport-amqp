<?php

declare(strict_types=1);

namespace Thesis\MessageBus\Transport\Amqp;

use Thesis\MessageBus\Stamp;

/**
 * @api
 */
final readonly class AmqpAppId implements Stamp
{
    public function __construct(
        public string $appId,
    ) {}
}
