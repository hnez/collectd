#include <time.h>
#include <unistd.h>

#include "collectd.h"

#include "plugin.h"
#include "utils/common/common.h"
#include "daemon/utils_random.h"

static char const *config_keys[] = {"Blocking", "NonBlocking", "TriggerAsan"};
static int config_keys_num = STATIC_ARRAY_SIZE(config_keys);

static int blocking_writers = 0;
static int non_blocking_writers = 0;

static char *gauge_names[] = {
  "alice",
  "bob",
  "carol",
  "dave",
  "eve",
  "malory",
  "oscar",
  "peggy",
  "trudy",
  "trent",
  NULL
};

static int wl_config(const char *key, const char *value) {
  if (strcasecmp(key, "Blocking") == 0) {
    blocking_writers = atoi(value);
    return 0;
  }

  if (strcasecmp(key, "NonBlocking") == 0) {
    non_blocking_writers = atoi(value);
    return 0;
  }

  if (strcasecmp(key, "TriggerAsan") == 0) {
    if (strcasecmp(value, "true") == 0) {
      int canary[20];
      canary[20] = 1;
      return canary[20];
    }
  }

  return -1;
}

static int wl_write_block(metric_family_t const *fam,
                    __attribute__((unused)) user_data_t *user_data) {
  // Consume an element from the queue on average once every two seconds
  // (e.g. slower than wl_read produces them -> stall)
  usleep(cdrand_range(1000000, 3000000));

  return 0;
}

static int wl_write_non_block(metric_family_t const *fam,
                    __attribute__((unused)) user_data_t *user_data) {
  // Consume on average two elements per second (e.g. faster than wl_read
  // produces them -> no stall)
  usleep(cdrand_range(0, 1000000));

  return 0;
}

static int wl_read(void) {
  static int prev_time = 0;
  int curr_time = time(NULL);

  if (prev_time == 0) {
    prev_time = curr_time;
  }

  for (char **name = gauge_names; *name != NULL && prev_time < curr_time; prev_time++, name++) {
    metric_family_t fam = {
      .name = *name,
      .type = METRIC_TYPE_GAUGE,
    };

    metric_family_metric_append(&fam, (metric_t){
      .value.gauge = prev_time,
    });

    int status = plugin_dispatch_metric_family(&fam);
    if (status != 0) {
      ERROR("write_throttle plugin: plugin_dispatch_metric_family failed: %s",
            STRERROR(status));
    }

    metric_family_metric_reset(&fam);
  }

  return 0;
}

static int wl_init(void) {
  printf("Spawning %d write threads that will block\n", blocking_writers);
  for (int i=0; i<blocking_writers; i++) {
    plugin_register_write("write_throttle_block", wl_write_block, NULL);
  }

  printf("Spawning %d write threads that will not block\n", non_blocking_writers);
  for (int i=0; i<non_blocking_writers; i++) {
    plugin_register_write("write_throttle_non_block", wl_write_non_block, NULL);
  }

  plugin_register_read("write_throttle_generator", wl_read);

  return 0;
}

void module_register(void) {
  plugin_register_config("write_throttle", wl_config, config_keys,
                         config_keys_num);
  plugin_register_init("write_throttle", wl_init);
}
