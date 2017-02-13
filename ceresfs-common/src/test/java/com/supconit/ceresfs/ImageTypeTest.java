package com.supconit.ceresfs;

import org.junit.Test;

import static org.junit.Assert.*;

public class ImageTypeTest {

    @Test
    public void testFromFileName() throws Exception {
        assertEquals(ImageType.BMP, ImageType.fromFileName("test.bmp"));
        assertEquals(ImageType.JPG, ImageType.fromFileName("test.jpg"));
        assertEquals(ImageType.JPG, ImageType.fromFileName("test.jpeg"));
        assertEquals(ImageType.GIF, ImageType.fromFileName("test.gif"));
        assertEquals(ImageType.PNG, ImageType.fromFileName("test.png"));

        try {
            ImageType.fromFileName("test.txt");
            fail();
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
        }

        try {
            // file without suffix
            ImageType.fromFileName("test");
            fail();
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
        }
    }

    @Test
    public void testFromCode() throws Exception {
        assertEquals(ImageType.JPG, ImageType.fromCode((byte) 1));
        assertEquals(ImageType.PNG, ImageType.fromCode((byte) 2));
        assertEquals(ImageType.GIF, ImageType.fromCode((byte) 3));
        assertEquals(ImageType.BMP, ImageType.fromCode((byte) 4));

        try {
            ImageType.fromCode((byte) 5);
            fail();
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
        }
    }
    
    @Test
    public void testFromMimeType() throws Exception {
        assertEquals(ImageType.JPG, ImageType.fromMimeType("image/jpeg"));
        assertEquals(ImageType.PNG, ImageType.fromMimeType("image/png"));
        assertEquals(ImageType.BMP, ImageType.fromMimeType("image/bmp"));
        assertEquals(ImageType.GIF, ImageType.fromMimeType("image/gif"));
    }
}