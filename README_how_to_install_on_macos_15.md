# How to Build fmptools on macOS 15 (Sequoia)

This guide walks through building fmptools from source on macOS 15 Sequoia with Apple Silicon.

## Prerequisites

You'll need Homebrew installed. If you don't have it, install from [brew.sh](https://brew.sh).

## Step 1: Clone the Repository

```bash
git clone https://github.com/evanmiller/fmptools.git
cd fmptools
```

## Step 2: Install Build Dependencies

Install the required build tools via Homebrew:

```bash
# Install automake (includes aclocal)
brew install automake

# Install libtool for building shared libraries
brew install libtool

# Install gettext (provides AM_ICONV macro)
# Note: gettext is usually already installed
brew install gettext
```

## Step 3: Set Up Build Environment

Create the m4 directory and copy necessary macro files:

```bash
# Create m4 directory for autoconf macros
mkdir -p m4

# Copy gettext/iconv m4 files
cp /opt/homebrew/Cellar/gettext/*/share/gettext/m4/*.m4 m4/ 2>/dev/null || \
cp /opt/homebrew/Cellar/gettext/*/share/aclocal/*.m4 m4/ 2>/dev/null
```

## Step 4: Configure the Build System

Add necessary configuration to the autotools files:

1. Edit `configure.ac` to add the macro directory configuration (add after the AC_INIT line):
   ```
   AC_CONFIG_MACRO_DIRS([m4])
   ```

2. Edit `Makefile.am` to add ACLOCAL flags (add after AUTOMAKE_OPTIONS):
   ```
   ACLOCAL_AMFLAGS = -I m4
   ```

## Step 5: Generate Configure Script

Run autoreconf to generate the configure script:

```bash
autoreconf -i -f
```

You should see output about copying libtool files and installing auxiliary files.

## Step 6: Configure the Build

```bash
./configure
```

This will check for dependencies and configure the build. The output will show which tools can be built based on available libraries.

## Step 7: Build the Tools

```bash
make
```

This will compile the library and available tools.

## What Gets Built

### Always Built
- **libfmptools** - Core library for reading FileMaker files
- **fmpdump** - Tool to dump information from FileMaker files
- **fmp2sqlite** - Convert FileMaker to SQLite (SQLite is included with macOS)

### Optional Tools (require additional libraries)
- **fmp2excel** - Convert to Excel format
  - Requires: `brew install libxlsxwriter`
- **fmp2json** - Convert to JSON format
  - Requires: `brew install yajl`

## Installing Optional Dependencies

To build all tools:

```bash
# For Excel support
brew install libxlsxwriter

# For JSON support
brew install yajl

# Reconfigure and rebuild
./configure
make clean
make
```

## Usage

After building, you can run the tools from the build directory:

```bash
# Convert FileMaker database to SQLite
./fmp2sqlite input.fp7 output.db

# Dump FileMaker database information
./fmpdump input.fp7

# If built with optional libraries:
./fmp2excel input.fp7 output.xlsx
./fmp2json input.fp7 > output.json
```

## System Installation (Optional)

To install the tools system-wide:

```bash
sudo make install
```

By default, this installs to `/usr/local/bin`.

## Supported FileMaker Formats

- fp3 (FileMaker Pro 3.x)
- fp5 (FileMaker Pro 5.x/6.x)
- fp7 (FileMaker Pro 7.x-11.x)
- fmp12 (FileMaker Pro 12.x and later)

## Troubleshooting

### "aclocal: command not found"
- Solution: Install automake: `brew install automake`

### "AM_ICONV not found"
- Solution: Install gettext and copy m4 files as shown in Step 3

### "LIBTOOL is undefined"
- Solution: Install libtool: `brew install libtool`

### Configure can't find optional libraries
- Make sure they're installed via Homebrew
- Try specifying paths: `./configure LDFLAGS="-L/opt/homebrew/lib" CPPFLAGS="-I/opt/homebrew/include"`

## Notes for Apple Silicon Macs

Homebrew on Apple Silicon installs to `/opt/homebrew` instead of `/usr/local`. The build process should automatically find libraries there, but if you have issues, you may need to add Homebrew to your PATH:

```bash
export PATH="/opt/homebrew/bin:$PATH"
```

## License

See the LICENSE file in the repository for details.