package com.supconit.ceresfs.compact;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

@Component
public class MarkCopyVolumeCompactor implements VolumeCompactor, InitializingBean, DisposableBean {

    @Override
    public void afterPropertiesSet() throws Exception {

    }

    @Override
    public void destroy() throws Exception {

    }
}
