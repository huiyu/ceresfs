package com.supconit.ceresfs.infrastructure;

import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Standalone infrastructure
 *
 * @author yuhui@supcon.com
 */
@Component
@ConditionalOnMissingBean(DistributedInfrastructure.class)
public class StandaloneInfrastructure implements Infrastructure, InitializingBean {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${port:9900}")
    private int port;
    @Value("${directories:/tmp:1}")
    private String directories;

    private Host localHost;

    @Override
    public Mode getMode() {
        return Mode.STANDALONE;
    }

    @Override
    public Host getLocalHost() {
        return localHost;
    }

    @Override
    public List<Host> getAllHosts() {
        List<Host> hosts = new ArrayList<>(1);
        hosts.add(localHost);
        return hosts;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        InetAddress localhost = InetAddress.getLocalHost();
        Host host = new Host();
        host.setName(localhost.getHostName());
        host.setPort(port);
        host.setAddress(localhost.getHostAddress());
        Map<String, String> weightByPath = Splitter.on(";").trimResults().withKeyValueSeparator(":").split(this.directories);
        List<Directory> directories = weightByPath.keySet().stream().map(path -> {
            String weight = weightByPath.get(path);
            return new Directory(path, StringUtils.isEmpty(weight) ? 1.0 : Double.parseDouble(weight));
        }).collect(Collectors.toList());
        host.setDirectories(directories);
        this.localHost = host;
        logger.info("Standalone infrastructure initialized at {}:{}", localHost.getAddress(), localHost.getPort());
    }
}
