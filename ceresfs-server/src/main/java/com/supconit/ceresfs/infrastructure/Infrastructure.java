package com.supconit.ceresfs.infrastructure;

import java.util.List;

public interface Infrastructure {

    enum Mode {

        STANDALONE,

        DISTRIBUTED,
    }

    Mode getMode();

    Host getLocalHost();

    List<Host> getAllHosts();
}
