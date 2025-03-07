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
package io.trino.spi.block;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkReadablePosition;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.block.BlockUtil.compactArray;
import static io.trino.spi.block.Int128ArrayBlock.INT128_BYTES;
import static java.lang.Math.max;

public class Int128ArrayBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(Int128ArrayBlockBuilder.class);
    private static final Block NULL_VALUE_BLOCK = new Int128ArrayBlock(0, 1, new boolean[] {true}, new long[2]);

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;
    private boolean initialized;
    private final int initialEntryCount;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    // it is assumed that these arrays are the same length
    private boolean[] valueIsNull = new boolean[0];
    private long[] values = new long[0];

    private long retainedSizeInBytes;

    public Int128ArrayBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.initialEntryCount = max(expectedEntries, 1);

        updateDataSize();
    }

    public void writeInt128(long high, long low)
    {
        if (valueIsNull.length <= positionCount) {
            growCapacity();
        }

        int valueIndex = positionCount * 2;
        values[valueIndex] = high;
        values[valueIndex + 1] = low;

        hasNonNullValue = true;
        positionCount++;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Int128ArrayBlock.SIZE_IN_BYTES_PER_POSITION);
        }
    }

    @Override
    public BlockBuilder appendNull()
    {
        if (valueIsNull.length <= positionCount) {
            growCapacity();
        }

        valueIsNull[positionCount] = true;

        hasNullValue = true;
        positionCount++;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Int128ArrayBlock.SIZE_IN_BYTES_PER_POSITION);
        }
        return this;
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, positionCount);
        }
        return new Int128ArrayBlock(0, positionCount, hasNullValue ? valueIsNull : null, values);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(int expectedEntries, BlockBuilderStatus blockBuilderStatus)
    {
        return new Int128ArrayBlockBuilder(blockBuilderStatus, expectedEntries);
    }

    private void growCapacity()
    {
        int newSize;
        if (initialized) {
            newSize = BlockUtil.calculateNewArraySize(valueIsNull.length);
        }
        else {
            newSize = initialEntryCount;
            initialized = true;
        }

        valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        values = Arrays.copyOf(values, newSize * 2);
        updateDataSize();
    }

    private void updateDataSize()
    {
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
        if (blockBuilderStatus != null) {
            retainedSizeInBytes += BlockBuilderStatus.INSTANCE_SIZE;
        }
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        return OptionalInt.of(Int128ArrayBlock.SIZE_IN_BYTES_PER_POSITION);
    }

    @Override
    public long getSizeInBytes()
    {
        return Int128ArrayBlock.SIZE_IN_BYTES_PER_POSITION * (long) positionCount;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return Int128ArrayBlock.SIZE_IN_BYTES_PER_POSITION * (long) length;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions, int selectedPositionsCount)
    {
        return Int128ArrayBlock.SIZE_IN_BYTES_PER_POSITION * (long) selectedPositionsCount;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return isNull(position) ? 0 : INT128_BYTES;
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(values, sizeOf(values));
        consumer.accept(valueIsNull, sizeOf(valueIsNull));
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkReadablePosition(this, position);
        if (offset == 0) {
            return values[position * 2];
        }
        if (offset == 8) {
            return values[(position * 2) + 1];
        }
        throw new IllegalArgumentException("offset must be 0 or 8");
    }

    @Override
    public boolean mayHaveNull()
    {
        return hasNullValue;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(this, position);
        return valueIsNull[position];
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(this, position);
        return new Int128ArrayBlock(
                0,
                1,
                valueIsNull[position] ? new boolean[] {true} : null,
                new long[] {
                        values[position * 2],
                        values[(position * 2) + 1]});
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        if (!hasNonNullValue) {
            return RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, length);
        }
        boolean[] newValueIsNull = null;
        if (hasNullValue) {
            newValueIsNull = new boolean[length];
        }
        long[] newValues = new long[length * 2];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(this, position);
            if (hasNullValue) {
                newValueIsNull[i] = valueIsNull[position];
            }
            newValues[i * 2] = values[(position * 2)];
            newValues[(i * 2) + 1] = values[(position * 2) + 1];
        }
        return new Int128ArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        if (!hasNonNullValue) {
            return RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, length);
        }
        return new Int128ArrayBlock(positionOffset, length, hasNullValue ? valueIsNull : null, values);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        if (!hasNonNullValue) {
            return RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, length);
        }
        boolean[] newValueIsNull = null;
        if (hasNullValue) {
            newValueIsNull = compactArray(valueIsNull, positionOffset, length);
        }
        long[] newValues = compactArray(values, positionOffset * 2, length * 2);
        return new Int128ArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public String getEncodingName()
    {
        return Int128ArrayBlockEncoding.NAME;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("Int128ArrayBlockBuilder{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    Slice getValuesSlice()
    {
        return Slices.wrappedLongArray(values, 0, positionCount * 2);
    }
}
