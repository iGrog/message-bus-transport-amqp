<?php

declare(strict_types=1);

namespace Thesis\MessageBus\Transport\Amqp;

use Amp\Future;
use Thesis\Amqp\Channel;
use Thesis\Amqp\Client;
use Thesis\Amqp\Config;
use Thesis\Amqp\DeliveryMessage;
use Thesis\Amqp\PublishMessage;
use Thesis\Message\Command;
use Thesis\MessageBus\Envelope;
use Thesis\MessageBus\Transport\Transport;
use function Amp\async;

/**
 * @api
 */
final class AmqpTransport implements Transport
{
    /**
     * @var ?Future<Channel>
     */
    private ?Future $publishChannelFuture = null;

    private ?Channel $publishChannel = null;

    public function __construct(
        private readonly Config $config,
        private readonly ExchangeNaming $exchangeNaming = new MessageClassBasedExchangeNaming(),
        private readonly AmqpEnvelopeEncoder $encoder = new DefaultAmqpEnvelopeEncoder(),
    ) {}

    public function setup(string $endpoint, array $localMessages): void
    {
        $client = new Client($this->config);
        $channel = $client->channel();
        $channel->queueDeclare($endpoint, durable: true);

        foreach ($localMessages as $localMessage) {
            $exchange = $this->exchangeNaming->nameExchange($localMessage);
            $this->declareExchange($channel, $exchange);
            $channel->queueBind($endpoint, $exchange);
        }

        $channel->close();
        $client->disconnect();
    }

    /**
     * @var array<non-empty-string, true>
     */
    private array $declaredExchanges = [];

    /**
     * @param non-empty-string $exchange
     */
    private function declareExchange(Channel $channel, string $exchange): void
    {
        if (!isset($this->declaredExchanges[$exchange])) {
            $channel->exchangeDeclare($exchange, exchangeType: 'fanout', durable: true);
            $this->declaredExchanges[$exchange] = true;
        }
    }

    public function publish(array $envelopes): void
    {
        if ($this->publishChannel === null || $this->publishChannel->isClosed()) {
            $this->publishChannelFuture ??= async(function (): Channel {
                $channel = new Client($this->config)->channel();
                $channel->confirmSelect();

                return $channel;
            });

            try {
                $this->publishChannel = $this->publishChannelFuture->await();
            } finally {
                $this->publishChannelFuture = null;
            }
        }

        $channel = $this->publishChannel;
        $channel
            ->publishBatch(array_map(
                function (Envelope $envelope) use ($channel): PublishMessage {
                    $exchange = $this->exchangeNaming->nameExchange($envelope->messageClass);
                    $this->declareExchange($channel, $exchange);

                    return new PublishMessage(
                        message: $this->encoder->encodeEnvelope($envelope),
                        exchange: $exchange,
                        mandatory: $envelope->message instanceof Command,
                    );
                },
                $envelopes,
            ))
            ->await()
            ->ensureAllPublished();
    }

    public function consume(string $endpoint, \Closure $handler): \Closure
    {
        $client = new Client($this->config);
        $channel = $client->channel();
        $channel->qos(prefetchCount: 1);

        $consumerTag = $channel->consume(
            callback: function (DeliveryMessage $deliveryMessage) use ($handler): void {
                $handler($this->encoder->decodeEnvelope($deliveryMessage->message));
                $deliveryMessage->ack();
            },
            queue: $endpoint,
        );

        return static function () use ($client, $channel, $consumerTag): void {
            $channel->cancel($consumerTag);
            $channel->close();
            $client->disconnect();
        };
    }
}
