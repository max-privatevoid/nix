#include "input-accessor.hh"
#include "util.hh"

#include <atomic>

#include <zip.h>

namespace nix {

static std::atomic<size_t> nextNumber{0};

InputAccessor::InputAccessor()
    : number(++nextNumber)
{ }

// FIXME: merge with archive.cc.
const std::string narVersionMagic1 = "nix-archive-1";

static string caseHackSuffix = "~nix~case~hack~";

void InputAccessor::dumpPath(
    const Path & path,
    Sink & sink,
    PathFilter & filter)
{
    auto dumpContents = [&](PathView path)
    {
        // FIXME: pipe
        auto s = readFile(path);
        sink << "contents" << s.size();
        sink(s);
        writePadding(s.size(), sink);
    };

    std::function<void(const std::string & path)> dump;

    dump = [&](const std::string & path) {
        checkInterrupt();

        auto st = lstat(path);

        sink << "(";

        if (st.type == tRegular) {
            sink << "type" << "regular";
            if (st.isExecutable)
                sink << "executable" << "";
            dumpContents(path);
        }

        else if (st.type == tDirectory) {
            sink << "type" << "directory";

            /* If we're on a case-insensitive system like macOS, undo
               the case hack applied by restorePath(). */
            std::map<string, string> unhacked;
            for (auto & i : readDirectory(path))
                if (/* archiveSettings.useCaseHack */ false) { // FIXME
                    string name(i.first);
                    size_t pos = i.first.find(caseHackSuffix);
                    if (pos != string::npos) {
                        debug(format("removing case hack suffix from '%s'") % (path + "/" + i.first));
                        name.erase(pos);
                    }
                    if (!unhacked.emplace(name, i.first).second)
                        throw Error("file name collision in between '%s' and '%s'",
                            (path + "/" + unhacked[name]),
                            (path + "/" + i.first));
                } else
                    unhacked.emplace(i.first, i.first);

            for (auto & i : unhacked)
                if (filter(path + "/" + i.first)) {
                    sink << "entry" << "(" << "name" << i.first << "node";
                    dump(path + "/" + i.second);
                    sink << ")";
                }
        }

        else if (st.type == tSymlink)
            sink << "type" << "symlink" << "target" << readLink(path);

        else throw Error("file '%s' has an unsupported type", path);

        sink << ")";
    };

    sink << narVersionMagic1;
    dump(path);
}

struct FSInputAccessor : InputAccessor
{
    Path root;
    std::optional<PathSet> allowedPaths;

    FSInputAccessor(const Path & root, std::optional<PathSet> && allowedPaths)
        : root(root)
        , allowedPaths(allowedPaths)
    {
        if (allowedPaths) {
            for (auto & p : *allowedPaths) {
                assert(!hasPrefix(p, "/"));
                assert(!hasSuffix(p, "/"));
            }
        }
    }

    std::string readFile(PathView path) override
    {
        auto absPath = makeAbsPath(path);
        printError("READ %s", absPath);
        checkAllowed(absPath);
        return nix::readFile(absPath);
    }

    bool pathExists(PathView path) override
    {
        auto absPath = makeAbsPath(path);
        printError("EXISTS %s", absPath);
        return isAllowed(absPath) && nix::pathExists(absPath);
    }

    Stat lstat(PathView path) override
    {
        auto absPath = makeAbsPath(path);
        printError("LSTAT %s", absPath);
        checkAllowed(absPath);
        auto st = nix::lstat(absPath);
        return Stat {
            .type =
                S_ISREG(st.st_mode) ? tRegular :
                S_ISDIR(st.st_mode) ? tDirectory :
                S_ISLNK(st.st_mode) ? tSymlink :
                tMisc,
            .isExecutable = S_ISREG(st.st_mode) && st.st_mode & S_IXUSR
        };
    }

    DirEntries readDirectory(PathView path) override
    {
        auto absPath = makeAbsPath(path);
        printError("READDIR %s", absPath);
        checkAllowed(absPath);
        DirEntries res;
        for (auto & entry : nix::readDirectory(absPath)) {
            std::optional<Type> type;
            switch (entry.type) {
            case DT_REG: type = Type::tRegular; break;
            case DT_LNK: type = Type::tSymlink; break;
            case DT_DIR: type = Type::tDirectory; break;
            }
            if (isAllowed(absPath + "/" + entry.name))
                res.emplace(entry.name, type);
        }
        return res;
    }

    std::string readLink(PathView path) override
    {
        auto absPath = makeAbsPath(path);
        printError("READLINK %s", absPath);
        checkAllowed(absPath);
        return nix::readLink(absPath);
    }

    Path makeAbsPath(PathView path)
    {
        assert(hasPrefix(path, "/"));
        return canonPath(root + std::string(path));
    }

    void checkAllowed(PathView absPath)
    {
        if (!isAllowed(absPath))
            // FIXME: for Git trees, show a custom error message like
            // "file is not under version control or does not exist"
            throw Error("access to path '%s' is not allowed", absPath);
    }

    bool isAllowed(PathView absPath)
    {
        if (!isDirOrInDir(absPath, root))
            return false;

        if (allowedPaths) {
            // FIXME: make isDirOrInDir return subPath
            auto subPath = absPath.substr(root.size());
            if (hasPrefix(subPath, "/"))
                subPath = subPath.substr(1);

            if (subPath != "") {
                auto lb = allowedPaths->lower_bound((std::string) subPath);
                if (lb == allowedPaths->end()
                    || !isDirOrInDir("/" + *lb, "/" + (std::string) subPath))
                    return false;
            }
        }

        return true;
    }
};

ref<InputAccessor> makeFSInputAccessor(
    const Path & root,
    std::optional<PathSet> && allowedPaths)
{
    return make_ref<FSInputAccessor>(root, std::move(allowedPaths));
}

std::ostream & operator << (std::ostream & str, const SourcePath & path)
{
    str << path.path; // FIXME
    return str;
}

struct MemoryInputAccessorImpl : MemoryInputAccessor
{
    std::map<Path, std::string> files;

    std::string readFile(PathView path) override
    {
        auto i = files.find((Path) path);
        if (i == files.end())
            throw Error("file '%s' does not exist", path);
        return i->second;
    }

    bool pathExists(PathView path) override
    {
        auto i = files.find((Path) path);
        return i != files.end();
    }

    Stat lstat(PathView path) override
    {
        throw UnimplementedError("MemoryInputAccessor::lstat");
    }

    DirEntries readDirectory(PathView path) override
    {
        return {};
    }

    std::string readLink(PathView path) override
    {
        throw UnimplementedError("MemoryInputAccessor::readLink");
    }

    void addFile(PathView path, std::string && contents) override
    {
        files.emplace(path, std::move(contents));
    }
};

ref<MemoryInputAccessor> makeMemoryInputAccessor()
{
    return make_ref<MemoryInputAccessorImpl>();
}

std::string_view SourcePath::baseName() const
{
    // FIXME
    return path == "" || path == "/" ? "source" : baseNameOf(path);
}

struct cmp_str
{
    bool operator ()(const char * a, const char * b) const
    {
        return std::strcmp(a, b) < 0;
    }
};

struct ZipMember
{
    struct zip_file * p = nullptr;
    ZipMember(struct zip_file * p) : p(p) { }
    ~ZipMember() { if (p) zip_fclose(p); }
    operator zip_file *() { return p; }
};

struct ZipInputAccessor : InputAccessor
{
    Path zipPath;
    struct zip * zipFile = nullptr;

    typedef std::map<const char *, struct zip_stat, cmp_str> Members;
    Members members;

    ZipInputAccessor(PathView _zipPath)
        : zipPath(_zipPath)
    {
        int error;
        zipFile = zip_open(zipPath.c_str(), 0, &error);
        if (!zipFile) {
            char errorMsg[1024];
            zip_error_to_str(errorMsg, sizeof errorMsg, error, errno);
            throw Error("couldn't open '%s': %s", zipPath, errorMsg);
        }

        /* Read the index of the zip file and put it in a map.  This
           is unfortunately necessary because libzip's lookup
           functions are O(n) time. */
        struct zip_stat sb;
        zip_uint64_t nrEntries = zip_get_num_entries(zipFile, 0);
        for (zip_uint64_t n = 0; n < nrEntries; ++n) {
            if (zip_stat_index(zipFile, n, 0, &sb))
                throw Error("couldn't stat archive member #%d in '%s': %s", n, zipPath, zip_strerror(zipFile));
            auto slash = strchr(sb.name, '/');
            if (!slash) continue;
            members.emplace(slash, sb);
        }
    }

    ~ZipInputAccessor()
    {
        if (zipFile) zip_close(zipFile);
    }

    std::string readFile(PathView _path) override
    {
        auto path = canonPath(_path);

        auto i = members.find(((std::string) path).c_str());
        if (i == members.end())
            throw Error("file '%s' does not exist", path);

        ZipMember member(zip_fopen_index(zipFile, i->second.index, 0));
        if (!member)
            throw Error("couldn't open archive member '%s' in '%s': %s",
                path, zipPath, zip_strerror(zipFile));

        std::string buf(i->second.size, 0);
        if (zip_fread(member, buf.data(), i->second.size) != (zip_int64_t) i->second.size)
            throw Error("couldn't read archive member '%s' in '%s'", path, zipPath);

        return buf;
    }

    bool pathExists(PathView _path) override
    {
        auto path = canonPath(_path);
        return members.find(((std::string) path).c_str()) != members.end();
    }

    Stat lstat(PathView _path) override
    {
        auto path = canonPath(_path);

        Type type = tRegular;
        bool isExecutable = false;

        auto i = members.find(((std::string) path).c_str());
        if (i == members.end()) {
            i = members.find(((std::string) path + "/").c_str());
            type = tDirectory;
        }
        if (i == members.end())
            throw Error("file '%s' does not exist", path);

        zip_uint8_t opsys;
        zip_uint32_t attributes;
        if (zip_file_get_external_attributes(zipFile, i->second.index, ZIP_FL_UNCHANGED, &opsys, &attributes) == -1)
            throw Error("couldn't get external attributes of '%s' in '%s': %s",
                path, zipPath, zip_strerror(zipFile));

        switch (opsys) {
        case ZIP_OPSYS_UNIX:
            auto type = (attributes >> 16) & 0770000;
            switch (type) {
            case 0040000: type = tDirectory; break;
            case 0100000:
                type = tRegular;
                isExecutable = (attributes >> 16) & 0000100;
                break;
            case 0120000: type = tSymlink; break;
            default:
                throw Error("file '%s' in '%s' has unsupported type %o", path, zipPath, type);
            }
            break;
        }

        return Stat { .type = type, .isExecutable = isExecutable };
    }

    DirEntries readDirectory(PathView _path) override
    {
        auto path = canonPath(_path) + "/";

        auto i = members.find(((std::string) path).c_str());
        if (i == members.end())
            throw Error("directory '%s' does not exist", path);

        ++i;

        DirEntries entries;

        for (; i != members.end() && strncmp(i->first, path.c_str(), path.size()) == 0; ++i) {
            auto start = i->first + path.size();
            auto slash = strchr(start, '/');
            if (slash && strcmp(slash, "/") != 0) continue;
            auto name = slash ? std::string(start, slash - start) : std::string(start);
            entries.emplace(name, std::nullopt);
        }

        return entries;
    }

    std::string readLink(PathView path) override
    {
        throw UnimplementedError("ZipInputAccessor::readLink");
    }
};

ref<InputAccessor> makeZipInputAccessor(PathView path)
{
    return make_ref<ZipInputAccessor>(path);
}

}
