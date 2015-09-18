package net.digitalbebop;

import com.google.gson.Gson;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.*;

public final class PulseFileUtils {
    private static Logger logger = LogManager.getLogger(PulseFileUtils.class);

    private static final String IMAGE_TAG = "image";

    private static class FormatObject {
        private final List<String> format;

        public FormatObject(List<String> format) {
            this.format = format;
        }
    }

    private static boolean isImageExtension(String ext) {
        switch (ext) {
            case "bmp":
            case "gif":
            case "jpg":
            case "jpeg":
            case "pcd":
            case "png":
            case "tif":
            case "tiff":
                return true;
        }

        return false;
    }

    public static String getMetaTags(@Nonnull Path path) {
        final Gson gson = new Gson();
        Set<String> formats = new HashSet<>();

        String ext = FilenameUtils.getExtension(path.toAbsolutePath().toString());
        if (isImageExtension(ext)) {
            formats.add(IMAGE_TAG);
        }

        List<String> outList = new ArrayList<>();
        outList.addAll(formats);

        return formats.isEmpty() ? "" : gson.toJson(new FormatObject(outList));
    }
}
