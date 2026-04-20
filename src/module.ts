import { AppPlugin } from '@grafana/data';
import { loadPluginCss } from '@grafana/runtime';

loadPluginCss({
  dark: 'plugins/dslimp-zabbix-clickhouse-app/styles/dark.css',
  light: 'plugins/dslimp-zabbix-clickhouse-app/styles/light.css',
});

export const plugin = new AppPlugin<{}>();
