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
package io.trino.plugin.hudi.files;

import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class FileSlice
{
    private final HudiFileGroupId fileGroupId;

    private final String baseInstantTime;

    private Optional<HudiBaseFile> baseFile;

    private final TreeSet<HudiLogFile> logFiles;

    public FileSlice(HudiFileGroupId fileGroupId, String baseInstantTime)
    {
        this.fileGroupId = fileGroupId;
        this.baseInstantTime = requireNonNull(baseInstantTime, "baseInstantTime is null");
        this.baseFile = Optional.empty();
        this.logFiles = new TreeSet<>(HudiLogFile.getReverseLogFileComparator());
    }

    public FileSlice(FileSlice fileSlice)
    {
        this.baseInstantTime = fileSlice.baseInstantTime;
        this.baseFile = fileSlice.baseFile.isPresent() ? Optional.of(new HudiBaseFile(fileSlice.baseFile.get())) : null;
        this.fileGroupId = fileSlice.fileGroupId;
        this.logFiles = new TreeSet<>(HudiLogFile.getReverseLogFileComparator());
        fileSlice.logFiles.forEach(lf -> this.logFiles.add(new HudiLogFile(lf)));
    }

    public FileSlice(String partitionPath, String baseInstantTime, String fileId)
    {
        this(new HudiFileGroupId(partitionPath, fileId), baseInstantTime);
    }

    public void setBaseFile(HudiBaseFile baseFile)
    {
        this.baseFile = Optional.ofNullable(baseFile);
    }

    public void addLogFile(HudiLogFile logFile)
    {
        this.logFiles.add(logFile);
    }

    public String getBaseInstantTime()
    {
        return baseInstantTime;
    }

    public Optional<HudiBaseFile> getBaseFile()
    {
        return baseFile;
    }

    public boolean isEmpty()
    {
        return (baseFile == null) && (logFiles.isEmpty());
    }

    public HudiFileGroupId getFileGroupId()
    {
        return fileGroupId;
    }

    public String getPartitionPath()
    {
        return fileGroupId.getPartitionPath();
    }

    public String getFileId()
    {
        return fileGroupId.getFileId();
    }

    public Stream<HudiLogFile> getLogFiles()
    {
        return logFiles.stream();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("fileGroupId=", fileGroupId)
                .add("baseInstantTime", baseInstantTime)
                .add("baseFile", baseFile)
                .add("logFiles", logFiles)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileSlice slice = (FileSlice) o;
        return Objects.equals(fileGroupId, slice.fileGroupId) && Objects.equals(baseInstantTime, slice.baseInstantTime)
                && Objects.equals(baseFile, slice.baseFile) && Objects.equals(logFiles, slice.logFiles);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fileGroupId, baseInstantTime);
    }
}
