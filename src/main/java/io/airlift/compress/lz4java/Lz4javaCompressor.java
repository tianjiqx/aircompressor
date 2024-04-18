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
package io.airlift.compress.lz4java;

import io.airlift.compress.Compressor;
import io.airlift.compress.lz4.Lz4RawCompressor;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.nio.ByteBuffer;

import static io.airlift.compress.lz4.Lz4RawCompressor.MAX_TABLE_SIZE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * This class is not thread-safe
 */
public class Lz4javaCompressor implements Compressor
{
    private static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
    private final int[] table = new int[MAX_TABLE_SIZE];
    private final LZ4Compressor compressor;

    public Lz4javaCompressor()
    {
        compressor = LZ4_FACTORY.fastCompressor();
//        System.out.println(compressor.getClass().getName());
    }

    private static void verifyRange(byte[] data, int offset, int length)
    {
        requireNonNull(data, "data is null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException(format("Invalid offset or length (%s, %s) in array of length %s", offset, length, data.length));
        }
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return Lz4RawCompressor.maxCompressedLength(uncompressedSize);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        return compressor.compress(input, 0, inputLength, output, 0, maxOutputLength);
    }

    @Override
    public void compress(ByteBuffer inputBuffer, ByteBuffer outputBuffer)
    {
        throw new RuntimeException("xx");
    }
}
