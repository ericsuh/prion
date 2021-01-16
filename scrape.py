from urllib.parse import urljoin, urlparse
import asyncio
import pathlib
import re
import sys

import httpx
import toml
from bs4 import BeautifulSoup


WAYBACK_RE = re.compile(
    r'https://web.archive.org/web/([^/]+)/'
    r'(?P<url>.*)$'
)

async def main(config_path):
    urls_queue = asyncio.Queue()
    request_pool = asyncio.Semaphore(1)  # Max requests
    visited_paths = set()
    client = httpx.AsyncClient()

    configs = read_configs(config_path)

    await urls_queue.put(configs['starting_url'])
    print('Starting loop', flush=True)
    while not urls_queue.empty() or len(asyncio.all_tasks()) > 1:
        if urls_queue.empty():
            await asyncio.sleep(1)
            continue
        url = await urls_queue.get()
        url = url.strip()
        path = get_subpath(configs['url_regex'], url)
        if not path:  # not a matching subpath
            continue
        # Test on path to skip
        if path in visited_paths:
            continue
        visited_paths.add(path)
        output_path = configs['output_dir'] / path
        ensure_folder(output_path)
        task = asyncio.create_task(handle_url(
            configs,
            client,
            request_pool,
            output_path,
            url,
            urls_queue,
        ))
    print('Stopping loop', flush=True)
    await client.aclose()


def read_configs(path):
    with open(path) as f:
        config = toml.load(f)
    assert 'url_regex' in config
    config['url_regex'] = re.compile(config['url_regex'])
    assert 'output_dir' in config
    assert 'starting_url' in config
    return config


async def handle_url(configs, client, request_pool, output_path, url, urls_queue):
    if output_path == configs['output_dir']:
        raise Exception(f'Invalid output path "{output_path}" for url {url}')
    try:
        async with request_pool:
            try:
                resp = await client.get(url)
            except Exception as e:
                resp = await try_wayback(client, url)
                if resp and resp.is_error:
                    print(f'HTTP Error ({resp.status_code}): {url}', flush=True)
                    return
                else:
                    print(f'Error ({e}): {url}', flush=True)
                    return
            if resp.is_error:
                resp = await try_wayback(client, url)
                if resp and resp.is_error:
                    print(f'HTTP Error ({resp.status_code}): {url}', flush=True)
                    return
                else:
                    print(f'Error (Unknown): {url}', flush=True)
                    return
            content_type = resp.headers['Content-Type']
            if 'text' in content_type or 'html' in content_type:
                await process_page(configs, output_path, url, urls_queue, resp.text)
            else:  # treat as binary content
                await process_file(configs, output_path, url, resp.content)
    except Exception:
        sys.excepthook(*sys.exc_info())
        sys.exit(1)


async def try_wayback(client, url):
    try:
        # Check if this URL is the wayback URL
        wayback_match = WAYBACK_RE.match(url)
        if wayback_match:
            url = wayback_match.group('url')
        resp = await client.get('http://archive.org/wayback/available?url=' + url)
        result = resp.json()
        if not result['archived_snapshots']:
            return None
        if not result['archived_snapshots']['closest']:
            return None
        if not result['archived_snapshots']['closest']['available']:
            return None
        new_url = result['archived_snapshots']['closest'].get('url')
        if not new_url:
            return None
        resp = await client.get(new_url)
        return resp
    except Exception:
        sys.excepthook(*sys.exc_info())
        return None


async def process_page(configs, output_path, current_url, urls_queue, text):
    if output_path == configs['output_dir']:
        raise Exception(f'Invalid output path "{output_path}" for url {url}')
    current_path = get_subpath(configs['url_regex'], current_url)

    soup = BeautifulSoup(text, 'lxml')
    base_url = None
    base_tag = soup.find('base')
    if base_tag:
        base_url = base_tag.get('href')

    tags = [('a', 'href'), ('frame', 'src'), ('img', 'src')]
    for tag, attr in tags:
        await process_url_tags(
            configs,
            base_url or current_url,
            current_path,
            urls_queue,
            soup,
            tag,
            attr
        )

    with open(output_path, 'w') as f:
        f.write(str(soup))


async def process_url_tags(configs, base_url, current_path, urls_queue, soup, tag, attr):
    for tag in soup.find_all(tag):
        tag_url = tag.get(attr)
        if not tag_url:
            continue
        parsed_url = urlparse(tag_url)
        if not parsed_url.scheme:
            tag_url = urljoin(base_url, tag_url)
        tag_path = get_subpath(configs['url_regex'], tag_url)
        if not tag_path:
            continue
        rel_path = path_relative(tag_path, current_path)
        tag[attr] = rel_path
        await urls_queue.put(tag_url)


async def process_file(configs, output_path, current_url, content):
    if output_path == configs['output_dir']:
        raise Exception(f'Invalid output path "{output_path}" for url {url}')
    current_path = get_subpath(configs['url_regex'], current_url)
    with open(output_path, 'wb') as f:
        f.write(content)


def get_subpath(url_regex, url):
    match = url_regex.match(url)
    if match:
        # Strip out fragments and query params
        url_path = urlparse(match.group('web_path')).path
        # Rough check that this isn't a scheme-less full URL like www.google.com
        if '.edu/' in url_path or '.com/' in url_path or '.org/' in url_path:
            return None
        # Make relative path
        path = pathlib.Path(url_path[1:])
        return path
    else:
        return None


def path_relative(path, relative_to):
    path = pathlib.Path(path)
    relative_to = pathlib.Path(relative_to)
    path_parts = path.parent.parts  # directory pieces
    base_parts = relative_to.parent.parts  # directory pieces
    prefix_len = 0
    while prefix_len < len(path_parts) and prefix_len < len(base_parts):
        if path_parts[prefix_len] == base_parts[prefix_len]:
            prefix_len += 1
            continue
        else:
            break
    rel_path = ['..'] * len(base_parts[prefix_len:]) + list(path_parts[prefix_len:])
    result = pathlib.Path(*rel_path, path.name)
    return result


def ensure_folder(path):
    folder = pathlib.Path(path).parent
    folder.mkdir(parents=True, exist_ok=True)


if __name__ == '__main__':
    asyncio.run(main(sys.argv[1]))
