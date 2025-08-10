use either::Either;


#[derive(Debug)]
pub struct GlobalConfig {
    config: Either<MasterConfig, SlaveConfig>,
    pub dir: Option<String>,
    pub dbfilename: Option<String>
}

impl GlobalConfig {
    pub fn new_from_master_config(config: MasterConfig, dir: Option<String>, dbfilename: Option<String>) -> Self {
        GlobalConfig {
            config: Either::Left(config),
            dir,
            dbfilename
        }
    }

    pub fn new_from_slave_config(config: SlaveConfig, dir: Option<String>, dbfilename: Option<String>) -> Self {
        GlobalConfig {
            config: Either::Right(config),
            dir,
            dbfilename
        }
    }

    pub fn get_master_config(&self) -> Option<&MasterConfig> {
        self.config.as_ref().left()
    }

    pub fn get_slave_config(&self) -> Option<&SlaveConfig> {
        self.config.as_ref().right()
    }

    pub fn role(&self) -> &str {
        if self.config.is_left() {
            "master"
        } else {
            "slave"
        }
    }
}

#[derive(Debug, Clone)]
pub struct SlaveConfig {
    pub master_address: String,
    pub master_port: usize,
}

impl SlaveConfig {
    pub fn new(master_address: String, master_port: usize) -> Self {
        SlaveConfig {
            master_address,
            master_port
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct MasterConfig {
    pub replication_id: String,
}

impl MasterConfig {
    pub fn new() -> Self {
        MasterConfig {
            replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string()
        }
    }
}
