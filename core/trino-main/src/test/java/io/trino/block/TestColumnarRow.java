/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.block;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import org.testng.annotations.Test;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;

import static io.trino.block.ColumnarTestUtils.alternatingNullValues;
import static io.trino.block.ColumnarTestUtils.assertBlock;
import static io.trino.block.ColumnarTestUtils.assertBlockPosition;
import static io.trino.block.ColumnarTestUtils.createTestDictionaryBlock;
import static io.trino.block.ColumnarTestUtils.createTestDictionaryExpectedValues;
import static io.trino.block.ColumnarTestUtils.createTestRleBlock;
import static io.trino.block.ColumnarTestUtils.createTestRleExpectedValues;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestColumnarRow
{
    private static final int POSITION_COUNT = 5;
    private static final int FIELD_COUNT = 5;

    @Test
    public void test()
    {
        Slice[][] expectedValues = new Slice[POSITION_COUNT][];
        for (int i = 0; i < POSITION_COUNT; i++) {
            expectedValues[i] = new Slice[FIELD_COUNT];
            for (int j = 0; j < FIELD_COUNT; j++) {
                if (j % 3 != 1) {
                    expectedValues[i][j] = Slices.utf8Slice(format("%d.%d", i, j));
                }
            }
        }
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        verifyBlock(blockBuilder, expectedValues);
        verifyBlock(blockBuilder.build(), expectedValues);

        Slice[][] expectedValuesWithNull = alternatingNullValues(expectedValues);
        BlockBuilder blockBuilderWithNull = createBlockBuilderWithValues(expectedValuesWithNull);
        verifyBlock(blockBuilderWithNull, expectedValuesWithNull);
        verifyBlock(blockBuilderWithNull.build(), expectedValuesWithNull);
    }

    private static <T> void verifyBlock(Block block, T[] expectedValues)
    {
        assertBlock(block, expectedValues);

        assertColumnarRow(block, expectedValues);
        assertDictionaryBlock(block, expectedValues);
        assertRunLengthEncodedBlock(block, expectedValues);

        int offset = 1;
        int length = expectedValues.length - 2;
        Block blockRegion = block.getRegion(offset, length);
        T[] expectedValuesRegion = Arrays.copyOfRange(expectedValues, offset, offset + length);

        assertBlock(blockRegion, expectedValuesRegion);

        assertColumnarRow(blockRegion, expectedValuesRegion);
        assertDictionaryBlock(blockRegion, expectedValuesRegion);
        assertRunLengthEncodedBlock(blockRegion, expectedValuesRegion);
    }

    private static <T> void assertDictionaryBlock(Block block, T[] expectedValues)
    {
        Block dictionaryBlock = createTestDictionaryBlock(block);
        T[] expectedDictionaryValues = createTestDictionaryExpectedValues(expectedValues);

        assertBlock(dictionaryBlock, expectedDictionaryValues);
        assertColumnarRow(dictionaryBlock, expectedDictionaryValues);
        assertRunLengthEncodedBlock(dictionaryBlock, expectedDictionaryValues);
    }

    private static <T> void assertRunLengthEncodedBlock(Block block, T[] expectedValues)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            RunLengthEncodedBlock runLengthEncodedBlock = createTestRleBlock(block, position);
            T[] expectedDictionaryValues = createTestRleExpectedValues(expectedValues, position);

            assertBlock(runLengthEncodedBlock, expectedDictionaryValues);
            assertColumnarRow(runLengthEncodedBlock, expectedDictionaryValues);
        }
    }

    private static <T> void assertColumnarRow(Block block, T[] expectedValues)
    {
        ColumnarRow columnarRow = toColumnarRow(block);
        assertEquals(columnarRow.getPositionCount(), expectedValues.length);

        for (int fieldId = 0; fieldId < FIELD_COUNT; fieldId++) {
            Block fieldBlock = columnarRow.getField(fieldId);
            int elementsPosition = 0;
            for (int position = 0; position < expectedValues.length; position++) {
                T expectedRow = expectedValues[position];
                assertEquals(columnarRow.isNull(position), expectedRow == null);
                if (expectedRow == null) {
                    continue;
                }

                Object expectedElement = Array.get(expectedRow, fieldId);
                assertBlockPosition(fieldBlock, elementsPosition, expectedElement);
                elementsPosition++;
            }
        }
    }

    public static BlockBuilder createBlockBuilderWithValues(Slice[][] expectedValues)
    {
        RowBlockBuilder blockBuilder = createBlockBuilder(null, 100);
        for (Slice[] expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.buildEntry(fieldBuilders -> {
                    for (int i = 0; i < expectedValue.length; i++) {
                        Slice v = expectedValue[i];
                        if (v == null) {
                            fieldBuilders.get(i).appendNull();
                        }
                        else {
                            VARCHAR.writeSlice(fieldBuilders.get(i), v);
                        }
                    }
                });
            }
        }
        return blockBuilder;
    }

    private static RowBlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return new RowBlockBuilder(Collections.nCopies(FIELD_COUNT, VARCHAR), blockBuilderStatus, expectedEntries);
    }
}
