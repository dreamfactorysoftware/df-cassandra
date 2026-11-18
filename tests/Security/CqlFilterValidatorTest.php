<?php

namespace DreamFactory\Core\Cassandra\Tests\Security;

use DreamFactory\Core\Cassandra\Resources\Table;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use PHPUnit\Framework\TestCase;

class CqlFilterValidatorTest extends TestCase
{
    /** @dataProvider validIdentifierProvider */
    public function testIdentifierAccepted(string $name): void
    {
        Table::assertSafeCqlIdentifier($name);
        $this->assertTrue(true);
    }

    public static function validIdentifierProvider(): array
    {
        return [
            ['col_name'],
            ['my_table'],
            ['_internal'],
            ['Column1'],
        ];
    }

    /** @dataProvider invalidIdentifierProvider */
    public function testIdentifierRejected(string $name): void
    {
        $this->expectException(InternalServerErrorException::class);
        Table::assertSafeCqlIdentifier($name);
    }

    public static function invalidIdentifierProvider(): array
    {
        return [
            ['col; SELECT'],
            ['col name'],
            ["col' OR '1"],
            ['col)'],
            ['1col'],
            [''],
        ];
    }

    public function testValueAcceptsScalars(): void
    {
        Table::assertSafeCqlFilterValue(42);
        Table::assertSafeCqlFilterValue('alice');
        Table::assertSafeCqlFilterValue(true);
        Table::assertSafeCqlFilterValue(null);
        $this->assertTrue(true);
    }

    /** @dataProvider unsafeValueProvider */
    public function testValueRejected($value): void
    {
        $this->expectException(InternalServerErrorException::class);
        Table::assertSafeCqlFilterValue($value);
    }

    public static function unsafeValueProvider(): array
    {
        return [
            ['hello; DROP'],
            ['x -- comment'],
            ['/* block */ 1'],
            ['x */'],
            ['back\\slash'],
            [['arr']],
        ];
    }

    public function testOperatorAccepted(): void
    {
        foreach (['=', '<', '>=', 'IN', 'CONTAINS', 'CONTAINS KEY'] as $op) {
            Table::assertSafeCqlComparisonOperator($op);
        }
        $this->assertTrue(true);
    }

    public function testOperatorRejected(): void
    {
        $this->expectException(InternalServerErrorException::class);
        Table::assertSafeCqlComparisonOperator('UNION SELECT');
    }
}
