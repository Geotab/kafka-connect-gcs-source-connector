package com.geotab.dna.dataplatform.kafka.connect.source.gcs.filesystems;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.FileSystemType;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

@Slf4j
public class FileSystemService {
  private final Map<FileSystemType, FileSystem> fileSystemMap;

  public FileSystemService(AbstractConfig config) {
    Reflections reflections = new Reflections("com.geotab.dna.dataplatform.kafka.connect.source.gcs.filesystems", new SubTypesScanner());
    Set<Class<? extends FileSystem>> subClasses = reflections.getSubTypesOf(FileSystem.class);
    fileSystemMap = subClasses.stream().map(subClass -> {
      try {
        FileSystem fileSystem = subClass.getDeclaredConstructor().newInstance();
        fileSystem.initialize(config);
        return fileSystem;
      } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
        log.error("cannot initialize the file system with exception {} {}", e.getMessage(), e.getCause());
        return null;
      }
    }).filter(Objects::nonNull).collect(Collectors.toConcurrentMap(FileSystem::getType, Function.identity()));

  }

  public FileSystem getFileSystem(FileSystemType fileSystemType) {
    return this.fileSystemMap.get(fileSystemType);
  }

}
