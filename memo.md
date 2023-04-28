`kibana.yml`ファイルは、Kibana のインストールディレクトリ内の`config`フォルダにあります。Kibana のインストールディレクトリは、インストール方法やプラットフォームによって異なります。以下は、一般的なプラットフォームでの`kibana.yml`ファイルの場所です。

1. Linux / macOS:
   Kibana が tar.gz ファイルや zip ファイルからインストールされた場合、`kibana.yml`ファイルは以下の場所にあります:

```
/path/to/your/kibana/installation/config/kibana.yml
```

`/path/to/your/kibana/installation`は、Kibana がインストールされたディレクトリへのパスに置き換えてください。

2. Debian / Ubuntu:
   Kibana が Debian パッケージからインストールされた場合、`kibana.yml`ファイルは以下の場所にあります:

```
/etc/kibana/kibana.yml
```

3. Windows:
   Kibana が zip ファイルからインストールされた場合、`kibana.yml`ファイルは以下の場所にあります:

```
C:\path\to\your\kibana\installation\config\kibana.yml
```

`C:\path\to\your\kibana\installation`は、Kibana がインストールされたディレクトリへのパスに置き換えてください。

`kibana.yml`ファイルには、Kibana の設定が含まれており、Elasticsearch への接続情報、ポート番号、ログ設定などを設定できます。変更を反映させるためには、Kibana を再起動する必要があります。
