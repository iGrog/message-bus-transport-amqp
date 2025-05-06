<?php

declare(strict_types=1);

namespace Thesis\MessageBus\Transport\Amqp;

use Thesis\Amqp\DeliveryMode;
use Thesis\MessageBus\Stamp;

/**
 * @api
 */
final readonly class AmqpDeliveryMode implements Stamp
{
    public function __construct(
        public DeliveryMode $deliveryMode,
    ) {}
}
